package s3

import (
	"fmt"
	"io"
	"net/url"
	netUrl "net/url"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v6"
)

type Service interface {
	AddLifeCycleRule(ruleId, folderPath string, daysToExpiry int) error
	UploadJSONFile(path string, data io.Reader) error
	UploadPDFFile(path string, data io.Reader) error
	GetFileUrl(path string, expiration time.Duration) (*url.URL, error)
	UploadJSONFileWithLink(path string, data io.Reader, linkExpiration time.Duration) (*url.URL, error)
	DownloadFile(path, localPath string) error
	DownloadDirectory(path, localPath string) error
	RemoveFile(path string) error
}

type service struct {
	s3Client       *minio.Client
	lifeCycleRules string
	bucketName     string
	urlValues      url.Values
}

func NewService(url, accessKey, accessSecret, bucketName string) (Service, error) {
	s3Client, err := minio.New(url, accessKey, accessSecret, true)
	if err != nil {
		return nil, err
	}
	exists, err := s3Client.BucketExists(bucketName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("s3 bucket required for service (%s) doesn't exist", bucketName)
	}
	urlValues := make(netUrl.Values)
	urlValues.Set("response-content-disposition", "inline")
	return &service{
		s3Client:       s3Client,
		lifeCycleRules: "",
		bucketName:     bucketName,
		urlValues:      urlValues,
	}, nil
}

func (s *service) AddLifeCycleRule(ruleId, folderPath string, daysToExpiry int) error {
	if !strings.HasSuffix(folderPath, "/") {
		folderPath = folderPath + "/"
	}
	lifeCycleString := fmt.Sprintf(
		`<LifecycleConfiguration><Rule><ID>%s</ID><Prefix>%s</Prefix><Status>Enabled</Status><Expiration><Days>%d</Days></Expiration></Rule></LifecycleConfiguration>`,
		ruleId, folderPath, daysToExpiry)
	return s.s3Client.SetBucketLifecycle(s.bucketName, lifeCycleString)
}

func (s *service) UploadJSONFile(path string, data io.Reader) error {
	_, err := s.s3Client.PutObject(s.bucketName, path, data, -1, minio.PutObjectOptions{ContentType: "application/json"})
	return err
}

func (s *service) UploadPDFFile(path string, data io.Reader) error {
	_, err := s.s3Client.PutObject(s.bucketName, path, data, -1, minio.PutObjectOptions{ContentType: "application/pdf"})
	return err
}

func (s *service) GetFileUrl(path string, expiration time.Duration) (*url.URL, error) {
	return s.s3Client.PresignedGetObject(s.bucketName, path, expiration, s.urlValues)
}

func (s *service) UploadJSONFileWithLink(path string, data io.Reader, linkExpiration time.Duration) (*url.URL, error) {
	_, err := s.s3Client.PutObject(s.bucketName, path, data, -1, minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return nil, err
	}
	return s.s3Client.PresignedGetObject(s.bucketName, path, 24*time.Hour, s.urlValues)
}

func (s *service) DownloadDirectory(path, localPath string) error {
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s.s3Client.ListObjectsV2(s.bucketName, path, true, doneCh)
	wg := sync.WaitGroup{}
	errCh := make(chan error)
	for obj := range objectCh {
		if obj.Err != nil {
			return obj.Err
		}
		wg.Add(1)
		go func(obj minio.ObjectInfo, errChan chan<- error) {
			fileName := strings.TrimPrefix(obj.Key, path+"/")
			err := s.DownloadFile(obj.Key, localPath+"/"+fileName)
			if err != nil {
				errCh <- err
			}
			wg.Done()
		}(obj, errCh)
	}
	wg.Wait()
	close(errCh)
	errs := []error{}
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("Failed to download files from s3: %v", errs)
	}
	return nil
}

func (s *service) DownloadFile(path, localPath string) error {
	return s.s3Client.FGetObject(s.bucketName, path, localPath, minio.GetObjectOptions{})
}

func (s *service) RemoveFile(path string) error {
	return s.s3Client.RemoveObject(s.bucketName, path)
}