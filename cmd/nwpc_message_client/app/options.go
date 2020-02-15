package app

import (
	"time"
)

var (
	brokerAddress  = ""
	disableDeliver = false

	enableProfiling  = false
	profilingAddress = "127.0.0.1:31485"

	writeTimeOut = 2 * time.Second

	Version   = "Unknown version"
	BuildTime = "Unknown build time"
	GitCommit = "Unknown GitCommit"
)
