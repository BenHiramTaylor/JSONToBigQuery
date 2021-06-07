package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func GetStorageClient(credsMap map[string]interface{}) (*storage.Client, error) {
	credsJSON, err := json.Marshal(credsMap)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON(credsJSON))
	if err != nil {
		return nil, err
	}
	return client, nil
}

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

func CreateBucket(client *storage.Client, projectID, bucketName string) error {
	ctx := context.Background()
	bkt := client.Bucket(bucketName)
	if err := bkt.Create(ctx, projectID, nil); err != nil {
		return err
	}
	return nil
}
