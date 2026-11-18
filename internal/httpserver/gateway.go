package httpserver

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"google.golang.org/protobuf/encoding/protojson"

	ledgerv1 "github.com/Kaaushikk/real-time-payments-ledger/gen/go/ledger/v1"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
)

// WithLedgerGateway mounts generated routes beside the existing HTTP API.
func WithLedgerGateway(ctx context.Context, conn *grpc.ClientConn, legacy http.Handler) (http.Handler, error) {
	gateway := runtime.NewServeMux(
		runtime.WithDisablePathLengthFallback(),
		runtime.WithMetadata(func(ctx context.Context, _ *http.Request) metadata.MD {
			return metadata.New(map[string]string(obs.TraceCarrier(ctx)))
		}),
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions: protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true},
		}),
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			// The gateway runtime forwards Authorization directly.
			return "idempotency-key", key == "Idempotency-Key"
		}),
	)
	if err := ledgerv1.RegisterLedgerServiceHandler(ctx, gateway, conn); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	gatewayHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Reject values the gateway runtime would drop as invalid metadata.
		for _, key := range r.Header.Values("Idempotency-Key") {
			if strings.IndexFunc(key, func(r rune) bool { return r < ' ' || r > '~' }) >= 0 {
				writeErr(w, http.StatusBadRequest, errors.New("idempotency header must contain printable ASCII"))
				return
			}
		}
		gateway.ServeHTTP(w, r)
	})
	mux.Handle("/gateway/", otelhttp.NewHandler(http.MaxBytesHandler(gatewayHandler, 16<<10), "ledger.gateway.http"))
	mux.Handle("/", legacy)
	return mux, nil
}
