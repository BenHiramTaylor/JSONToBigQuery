package gcp

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func GetClient(credsJSON []byte) (storage.Client, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON(credsJSON))
	if err != nil {
		return *client, err
	}
	return *client, nil
}

func DownloadBlobFromStorage(client storage.Client, bucketName, dataset, fileName string) error {
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

func CreateBucket(client storage.Client, projectID, bucketName string) error {
	ctx := context.Background()
	bkt := client.Bucket(bucketName)
	if err := bkt.Create(ctx, projectID, nil); err != nil {
		return err
	}
	return nil
}
