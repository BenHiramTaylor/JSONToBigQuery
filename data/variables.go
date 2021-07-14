package data

import (
	"os"
)

// Static global variables
var (
	// BucketName The google storage bucket that the AVSC files are stored in
	BucketName = "jtb-source-structures"
	// CredsFilePath Gets the file path for the key.json from the env
	CredsFilePath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
)
