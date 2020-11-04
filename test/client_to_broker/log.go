package client_to_broker

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

func CreateWorkerLog(index int, logDir string) (*log.Logger, *os.File) {
	var workerLog = log.New()

	logName := fmt.Sprintf("worker.%d.log", index)
	logPath := filepath.Join(logDir, logName)
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		workerLog.SetOutput(file)
	} else {
		workerLog.Fatal("Failed to log to file, using default stderr: %v", err)
		return nil, file
	}
	return workerLog, file
}
