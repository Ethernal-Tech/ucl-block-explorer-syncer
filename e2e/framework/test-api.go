package framework

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type API struct {
	node     *node
	config   ApiConfig
	dbConfig DBConfig
	logsDir  string
	t        *testing.T
}

func NewAPI(t *testing.T, cfg ApiConfig, dbCfg DBConfig, logsDir string) *API {
	t.Helper()

	if cfg.Listen == "" {
		cfg.Listen = "0.0.0.0:8545"
	}

	return &API{t: t, config: cfg, dbConfig: dbCfg, logsDir: logsDir}
}

func (a *API) Start() {
	f, err := os.OpenFile(filepath.Join(a.logsDir, "api.log"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		a.t.Fatalf("failed to create api log file: %v", err)
	}

	args := []string{
		"run", ".", "api",
		"--listen", a.config.Listen,
		"--db-conn", a.dbConfig.ConnString(),
	}

	if a.config.Logging {
		args = append(args, "--logging")
	}

	if a.config.AdminSecret != "" {
		args = append(args, "--admin-api-secret", a.config.AdminSecret)
	}

	n, err := newNode("go", args, f, "..")
	if err != nil {
		a.t.Fatalf("failed to start api: %v", err)
	}

	a.node = n
	a.waitReady(30 * time.Second)
}

func (a *API) Stop() {
	if a.node == nil || a.node.cmd == nil {
		return
	}

	syscall.Kill(-a.node.cmd.Process.Pid, syscall.SIGTERM) //nolint:errcheck

	select {
	case <-a.node.Wait():
	case <-time.After(10 * time.Second):
		syscall.Kill(-a.node.cmd.Process.Pid, syscall.SIGKILL) //nolint:errcheck
	}
}

func (a *API) IsRunning() bool {
	return a.node != nil && a.node.cmd != nil
}

func (a *API) URL() string {
	return fmt.Sprintf("http://%s", a.config.Listen)
}

func (a *API) waitReady(timeout time.Duration) {
	deadline := time.Now().UTC().Add(timeout)
	for time.Now().UTC().Before(deadline) {
		resp, err := http.Get(a.URL())
		if err == nil {
			resp.Body.Close() //nolint:errcheck
			a.t.Log("api ready")

			return
		}

		time.Sleep(time.Second)
	}

	a.t.Fatal("api not ready after timeout")
}

func (a *API) AddERC20ToWatchlist(address, symbol string, decimals int, secret string) {
	body, err := json.Marshal(map[string]interface{}{
		"address":  address,
		"symbol":   symbol,
		"decimals": decimals,
		"enabled":  true,
	})
	if err != nil {
		a.t.Fatalf("failed to marshal watchlist request: %v", err)
	}

	req, err := http.NewRequest("POST",
		fmt.Sprintf("%s/admin/v1/erc20/watchlist", a.URL()),
		bytes.NewReader(body))
	if err != nil {
		a.t.Fatalf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("failed to add erc20 to watchlist: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		a.t.Fatalf("failed to add erc20 to watchlist: status=%d body=%s", resp.StatusCode, respBody)
	}
}

func (a *API) RemoveERC20FromWatchlist(address, secret string) {
	body, err := json.Marshal(map[string]interface{}{
		"address": address,
		"enabled": false,
	})
	if err != nil {
		a.t.Fatalf("failed to marshal watchlist request: %v", err)
	}

	req, err := http.NewRequest("POST",
		fmt.Sprintf("%s/admin/v1/erc20/watchlist", a.URL()),
		bytes.NewReader(body))
	if err != nil {
		a.t.Fatalf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("failed to remove erc20 from watchlist: %v", err)
	}

	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		a.t.Fatalf("failed to remove erc20 from watchlist: status=%d body=%s", resp.StatusCode, respBody)
	}
}

func (a *API) UpsertValidator(address, name, institution, region, secret string) {
	body, err := json.Marshal(map[string]interface{}{
		"name":        name,
		"institution": institution,
		"region":      region,
	})
	if err != nil {
		a.t.Fatalf("failed to marshal validator request: %v", err)
	}

	req, err := http.NewRequest("PUT",
		fmt.Sprintf("%s/admin/v1/validators/%s", a.URL(), address),
		bytes.NewReader(body))
	if err != nil {
		a.t.Fatalf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("failed to upsert validator: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		a.t.Fatalf("failed to upsert validator: status=%d body=%s", resp.StatusCode, respBody)
	}
}

func (a *API) DeleteValidator(address, secret string) {
	req, err := http.NewRequest("DELETE",
		fmt.Sprintf("%s/admin/v1/validators/%s", a.URL(), address),
		nil)
	if err != nil {
		a.t.Fatalf("failed to create request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("failed to delete validator: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		a.t.Fatalf("failed to delete validator: status=%d body=%s", resp.StatusCode, respBody)
	}
}

func (a *API) CreateAssetIssuer(name, website, contact, region, secret string, assets []string) string {
	body, err := json.Marshal(map[string]interface{}{
		"name":    name,
		"website": website,
		"contact": contact,
		"region":  region,
		"assets":  assets,
	})
	if err != nil {
		a.t.Fatalf("failed to marshal asset issuer request: %v", err)
	}

	req, err := http.NewRequest("POST",
		fmt.Sprintf("%s/admin/v1/asset-issuers", a.URL()),
		bytes.NewReader(body))
	if err != nil {
		a.t.Fatalf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("failed to create asset issuer: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		a.t.Fatalf("failed to create asset issuer: status=%d body=%s", resp.StatusCode, respBody)
	}

	var result struct {
		OK bool   `json:"ok"`
		ID string `json:"id"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		a.t.Fatalf("failed to decode response: %v", err)
	}

	return result.ID
}

func (a *API) UpdateAssetIssuer(id, name, website, contact, region, secret string, assets []string) {
	body, err := json.Marshal(map[string]interface{}{
		"name":    name,
		"website": website,
		"contact": contact,
		"region":  region,
		"assets":  assets,
	})
	if err != nil {
		a.t.Fatalf("failed to marshal asset issuer request: %v", err)
	}

	req, err := http.NewRequest("PUT",
		fmt.Sprintf("%s/admin/v1/asset-issuers/%s", a.URL(), id),
		bytes.NewReader(body))
	if err != nil {
		a.t.Fatalf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("failed to update asset issuer: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		a.t.Fatalf("failed to update asset issuer: status=%d body=%s", resp.StatusCode, respBody)
	}
}

func (a *API) DeleteAssetIssuer(id, secret string) {
	req, err := http.NewRequest("DELETE",
		fmt.Sprintf("%s/admin/v1/asset-issuers/%s", a.URL(), id),
		nil)
	if err != nil {
		a.t.Fatalf("failed to create request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("failed to delete asset issuer: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		a.t.Fatalf("failed to delete asset issuer: status=%d body=%s", resp.StatusCode, respBody)
	}
}

func Call[T any](a *API, method string, params ...interface{}) (T, error) {
	var zero T

	if params == nil {
		params = []interface{}{}
	}

	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	})
	if err != nil {
		return zero, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := http.Post(a.URL(), "application/json", bytes.NewReader(body))
	if err != nil {
		return zero, fmt.Errorf("failed to call %s: %w", method, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, fmt.Errorf("failed to read response: %w", err)
	}

	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return zero, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if rpcResp.Error != nil {
		return zero, fmt.Errorf("rpc error [%d]: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	var result T
	if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
		return zero, fmt.Errorf("failed to unmarshal result into %T: %w", result, err)
	}

	return result, nil
}
