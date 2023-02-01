package pkg

import "cloud.google.com/go/bigquery"

type DataSource struct {
	SourceUri         string                          `yaml:"source_uri"`
	SourceFormat      bigquery.DataFormat             `yaml:"source_format"`
	AvroOptions       *AvroOptions                    `yaml:"avro_options"`
	CSVOptions        *CSVOptions                     `yaml:"csv_options"`
	ParquetOptions    *ParquetOptions                 `yaml:"parquet_options"`
	AutoDetect        bool                            `yaml:"auto_detect"`
	DatasetId         string                          `yaml:"dataset_id"`
	TableId           string                          `yaml:"table_id"`
	CreateDisposition bigquery.TableCreateDisposition `yaml:"create_disposition"`
	WriteDisposition  bigquery.TableWriteDisposition  `yaml:"write_disposition"`
}

type AvroOptions struct {
	UseAvroLogicalTypes bool `yaml:"use_avro_logical_types"`
}

type CSVOptions struct {
	AllowJaggedRows     bool              `yaml:"allow_jagged_rows"`
	AllowQuotedNewlines bool              `yaml:"allow_quoted_newlines"`
	Encoding            bigquery.Encoding `yaml:"encoding"`
	FieldDelimiter      string            `yaml:"field_delimiter"`
	Quote               string            `yaml:"quote"`
	ForceZeroQuote      bool              `yaml:"force_zero_quote"`
	SkipLeadingRows     int64             `yaml:"skip_leading_rows"`
	NullMarker          string            `yaml:"null_marker"`
}

type ParquetOptions struct {
	EnumAsString        bool `yaml:"enum_as_string"`
	EnableListInference bool `yaml:"enable_list_inference"`
}
