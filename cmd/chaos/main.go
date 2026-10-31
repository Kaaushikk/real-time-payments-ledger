package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	base := getenv("TOXIPROXY_URL", "http://localhost:8474")
	scenarios := []scenario{
		{Name: "1_postgres_latency_2s", Proxy: "postgres", Toxic: toxic("latency", map[string]any{"latency": 2000})},
		{Name: "2_postgres_latency_5s", Proxy: "postgres", Toxic: toxic("latency", map[string]any{"latency": 5000})},
		{Name: "3_postgres_timeout", Proxy: "postgres", Toxic: toxic("timeout", map[string]any{"timeout": 1000})},
		{Name: "4_postgres_reset_peer", Proxy: "postgres", Toxic: toxic("reset_peer", map[string]any{"timeout": 0})},
		{Name: "5_postgres_slicer", Proxy: "postgres", Toxic: toxic("slicer", map[string]any{"average_size": 64, "size_variation": 32, "delay": 100})},
		{Name: "6_postgres_bandwidth", Proxy: "postgres", Toxic: toxic("bandwidth", map[string]any{"rate": 1})},
		{Name: "7_redis_latency_2s", Proxy: "redis", Toxic: toxic("latency", map[string]any{"latency": 2000})},
		{Name: "8_redis_timeout", Proxy: "redis", Toxic: toxic("timeout", map[string]any{"timeout": 1000})},
		{Name: "9_redis_reset_peer", Proxy: "redis", Toxic: toxic("reset_peer", map[string]any{"timeout": 0})},
		{Name: "10_redis_down", Proxy: "redis", Down: true},
		{Name: "11_combined_latency", Both: true},
		{Name: "12_recover", Recover: true},
	}

	for _, s := range scenarios {
		fmt.Println("===", s.Name)
		reset(base)
		if s.Recover {
			fmt.Println("proxies restored")
			continue
		}
		if s.Both {
			postToxic(base, "postgres", toxic("latency", map[string]any{"latency": 1500}))
			postToxic(base, "redis", toxic("latency", map[string]any{"latency": 1500}))
		} else if s.Down {
			toggle(base, s.Proxy, false)
		} else {
			postToxic(base, s.Proxy, s.Toxic)
		}
		time.Sleep(200 * time.Millisecond)
	}
	reset(base)
	fmt.Println("all 12 scenarios configured then cleared")
}

type scenario struct {
	Name    string
	Proxy   string
	Toxic   map[string]any
	Down    bool
	Both    bool
	Recover bool
}

func toxic(typ string, attrs map[string]any) map[string]any {
	return map[string]any{"type": typ, "stream": "downstream", "toxicity": 1.0, "attributes": attrs}
}

func postToxic(base, proxy string, body map[string]any) {
	b, _ := json.Marshal(body)
	resp, err := http.Post(base+"/proxies/"+proxy+"/toxics", "application/json", bytes.NewReader(b))
	if err != nil {
		fmt.Println("error", err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func toggle(base, proxy string, enabled bool) {
	b, _ := json.Marshal(map[string]any{"enabled": enabled})
	req, _ := http.NewRequest(http.MethodPost, base+"/proxies/"+proxy, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(os.Stdout, resp.Body)
	fmt.Println()
}

func reset(base string) {
	for _, p := range []string{"postgres", "redis"} {
		toggle(base, p, true)
		req, _ := http.NewRequest(http.MethodGet, base+"/proxies/"+p+"/toxics", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		var toxics []struct{ Name string `json:"name"` }
		_ = json.NewDecoder(resp.Body).Decode(&toxics)
		resp.Body.Close()
		for _, t := range toxics {
			req, _ := http.NewRequest(http.MethodDelete, base+"/proxies/"+p+"/toxics/"+t.Name, nil)
			r, err := http.DefaultClient.Do(req)
			if err == nil {
				r.Body.Close()
			}
		}
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
