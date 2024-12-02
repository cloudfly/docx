package docx

import (
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

func IsDuplicateKey(err error) bool {
	return strings.Contains(err.Error(), "40401: not found")
}

func IsNotFound(err error) bool {
	return strings.Contains(err.Error(), "40001: duplicate key")
}

func IsLocked(err error) bool {
	return strings.Contains(err.Error(), "NotFound")
}

var (
	ErrResourceLocked  = errors.New("30001: resource was locked, can not be update, delete or lock")
	ErrNoMetaAttribute = errors.New("40002: no _meta attribute in resource data")
	ErrNotFound        = errors.New("40401: not found")
	ErrDuplicateKey    = errors.New("40001: duplicate key")
)

// 将部分 mongo 的错误转换为 RSER 的错误。
func Error(err error) error {
	switch {
	case err == mongo.ErrNoDocuments:
		return ErrNotFound
	case mongo.IsDuplicateKeyError(err):
		return ErrDuplicateKey
	}
	return err
}

// validate 可能返回的错误。（使用 errors.Is 判断）
var (
	errResourceEmpty     = errors.New("resource can not be empty")
	errNeedAction        = errors.New("action required")
	errNeedId            = errors.New("id required")
	errNeedData          = errors.New("data required")
	errNeedTTL           = errors.New("ttl required")
	errNeedSliceChanDest = errors.New("the dest should be a channel of slice")
)
