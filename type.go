package docx

import (
	"reflect"
	"time"
)

const (
	IdKey         = "_id"
	MetaKey       = "meta"
	DataKey       = "data"
	LockExpireKey = "lockExpireAt"
	LockSecretKey = "lockSecret"
	VersionKey    = "version"
	TagKey        = "tags"
	CreateAtKey   = "createAt"
	UpdateAtKey   = "updateAt"
	RawIdKey      = "rawId"
)

type Meta struct {
	RawId        string   `json:"-" bson:"rawId"`
	LockSecret   string   `json:"-" bson:"lockSecret"`
	LockExpireAt int64    `json:"lockExpireAt" bson:"lockExpireAt"`
	Tags         []string `json:"tags" bson:"tags"`
	Version      int64    `json:"version" bson:"version"`
	CreateAt     int64    `json:"createAt" bson:"createAt"`
	UpdateAt     int64    `json:"updateAt" bson:"updateAt"`
}

func DefaultMeta() *Meta {
	return &Meta{
		Version:  1,
		CreateAt: time.Now().Unix(),
		UpdateAt: time.Now().Unix(),
	}
}

// Resource 代表一个资源数据， Rser 与 Client 之间的交互使用 json 编码，与 mongo 之间使用 bson 编码
type Resource struct {
	Id   string `json:"id" bson:"_id"`
	Meta *Meta  `json:"meta,omitempty" bson:"meta"`
	Data any    `json:"-" bson:"data"`
}

type Index struct {
	Name       string   `json:"name"`
	Attributes []string `json:"attributes"`
	Unique     bool     `json:"unique"`
}

func (index Index) Equal(index2 Index) bool {
	return reflect.DeepEqual(index, index2)
}
