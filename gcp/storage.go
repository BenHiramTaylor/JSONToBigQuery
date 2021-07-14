package gcp

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// GetStorageClient Constructor func returns a Storage Client
func GetStorageClient(credsFilePath string) (*storage.Client, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(credsFilePath))
	if err != nil {
		return nil, err
	}
	return client, nil
}

// DownloadBlobFromStorage Downloads a file from Google storage and writes it to
// a local file of the same name
func DownloadBlobFromStorage(client *storage.Client, bucketName, dataset, fileName string) error {
	blobName := fmt.Sprintf("%v/%v", dataset, fileName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	r, err := client.Bucket(bucketName).Object(blobName).NewReader(ctx)
	if err != nil {
		return err
	}
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(blobName, data, 0644)
	if err != nil {
		return err
	}
	return nil
}

// UploadBlobToStorage Uploads a file to Google storage
func UploadBlobToStorage(client *storage.Client, bucketName, dataset, fileName string) error {
	blobName := fmt.Sprintf("%v/%v", dataset, fileName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	data, err := ioutil.ReadFile(blobName)
	w := client.Bucket(bucketName).Object(blobName).NewWriter(ctx)
	defer w.Close()
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	if err = os.Remove(blobName); err != nil {
		return err
	}
	log.Printf("uploaded %v to %v", blobName, bucketName)
	return nil
}

// CreateBucket Creates a Google storage bucket if it does not already exist
func CreateBucket(client *storage.Client, projectID, bucketName string) error {
	ctx := context.Background()
	bkt := client.Bucket(bucketName)
	if err := bkt.Create(ctx, projectID, nil); err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code != 409 {
				log.Printf("ERROR CREATING BUCKET: %v", err.Error())
				return nil
			}
		}
		return err
	}
	return nil
}
