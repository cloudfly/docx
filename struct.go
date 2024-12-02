package docx

const (
	schemeCollection       = "rser_scheme"
	versionCollection      = "rser_version"
	idIndexName            = "_id_"
	historyMetaIdIndexName = "idx_meta_id"
)

var (
	builtinIndexes = []Index{
		{
			Name:       "idx_meta_createAt",
			Attributes: []string{MetaKey + "." + CreateAtKey},
			Unique:     false,
		},
	}
)
