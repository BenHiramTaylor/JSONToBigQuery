package data

import (
	"os"
)

var (
	BucketName    = "jtb-source-structures"
	CredsFilePath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
)