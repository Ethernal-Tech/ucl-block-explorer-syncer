package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type TestSyncer struct {
	edgeCmd   *exec.Cmd
	syncerCmd *exec.Cmd
	t         *testing.T
}

func NewTestSyncer(t *testing.T) *TestSyncer {
	return &TestSyncer{t: t}
}

func (f *TestSyncer) StartUCL() {
	f.t.Helper()

	f.edgeCmd = exec.Command("bash", "./ucl/scripts/cluster.sh", "ibft", "write-logs")
	f.edgeCmd.Stdout = os.Stdout
	f.edgeCmd.Stderr = os.Stderr

	if err := f.edgeCmd.Start(); err != nil {
		f.t.Fatalf("failed to start edge: %v", err)
	}

	time.Sleep(10 * time.Second)
}

func (f *TestSyncer) StartSyncer() {
	f.t.Helper()

	f.syncerCmd = exec.Command("go", "run", "./cmd/syncer")
	f.syncerCmd.Stdout = os.Stdout
	f.syncerCmd.Stderr = os.Stderr

	if err := f.syncerCmd.Start(); err != nil {
		f.t.Fatalf("failed to start syncer: %v", err)
	}

	time.Sleep(5 * time.Second)
}

func (f *TestSyncer) Stop() {
	if f.syncerCmd != nil && f.syncerCmd.Process != nil {
		f.syncerCmd.Process.Kill()
	}
	if f.edgeCmd != nil && f.edgeCmd.Process != nil {
		f.edgeCmd.Process.Kill()
	}

	dockerComposeDown()
	dockerVolumePrune()
}

func dockerComposeUp(logsDir string) {
	// Create a log file for docker-compose output
	f, err := os.OpenFile(filepath.Join(logsDir, "zk-server.log"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("Error creating zk-server log file: %v\n", err)
	}

	// Equivalent to running 'docker compose up' in your terminal
	cmd := exec.Command("sudo", "docker", "compose", "up")

	// Set the working directory for this command
	cmd.Dir = "../docker/zk"
	cmd.Stdout = f
	cmd.Stderr = f

	if err := cmd.Start(); err != nil {
		fmt.Printf("Error executing docker compose up: %v\n", err)
	}

	if err := cmd.Process.Release(); err != nil {
		fmt.Printf("Error releasing docker compose process: %v\n", err)
	}

	fmt.Println("docker compose up executed")
}

func dockerComposeDown() {
	cmd := exec.Command("sudo", "docker", "compose", "down")
	cmd.Dir = "../docker/zk"

	if err := cmd.Start(); err != nil {
		fmt.Printf("Error executing docker compose down: %v\n", err)
	}

	if err := cmd.Wait(); err != nil {
		fmt.Printf("Error waiting for docker compose down to execute: %v\n", err)
	}

	fmt.Println("docker compose down executed")
}

func dockerVolumePrune() {
	cmd := exec.Command("sudo", "docker", "volume", "prune", "-f")

	if err := cmd.Start(); err != nil {
		fmt.Printf("Error executing docker volume prune: %v\n", err)
	}

	if err := cmd.Wait(); err != nil {
		fmt.Printf("Error waiting for docker volume prune to execute: %v\n", err)
	}

	fmt.Println("docker volume prune executed")
}
