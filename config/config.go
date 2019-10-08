package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/joeshaw/multierror"
)

type validator interface {
	Validate() error
}

func ReadConfig(c *common.Config, config interface{}) error {
	if err := c.Unpack(config); err != nil {
		return fmt.Errorf("failed unpacking config. %v", err)
	}

	var errs multierror.Errors
	// Check for invalid keys.
	if len(AalogbeatConfigKeys) > 0 {
		for _, k := range c.GetFields() {
			if !AalogbeatConfigKeys.Has(k) {
				errs = append(errs, fmt.Errorf("invalid log key '%s' "+
					"found. Valid keys are %s", k, strings.Join(AalogbeatConfigKeys.ToSlice(), ", ")))
			}
		}
	}

	if v, ok := config.(validator); ok {
		if err := v.Validate(); err != nil {
			errs = append(errs, err)
		}
	}

	return errs.Err()
}

const (
	// DefaultRegistryFile specifies the default filename of the registry file
	DefaultRegistryFile = ".aalogbeat.yml"
	// DefaultFilePattern specifies the default file extension for log files
	DefaultFilePattern = "*.aaLOG"
	// DefaultDirectory specifies the default directory that contains logs to monitor
	DefaultDirectory = "C:/ProgramData/ArchestrA/LogFiles"
	// DefaultBatchSize specifies the default number of log messages to read at a time
	DefaultBatchSize = 10000
)

var (
	DefaultSettings = AalogbeatConfig{
		RegistryFile: DefaultRegistryFile,
		FilePattern:  DefaultFilePattern,
		Directory:    DefaultDirectory,
		BatchSize:    DefaultBatchSize,
	}
)

var AalogbeatConfigKeys = common.MakeStringSet("batch_size", "file_pattern",
	"directory", "fields", "tags", "shutdown_timeout", "registry_file")

// AalogbeatConfig contains al of Aalogbeat configuration data
type AalogbeatConfig struct {
	Directory       string        `config:"directory"`
	FilePattern     string        `config:"file_pattern"`
	BatchSize       int           `config:"batch_size" validate:"min=1"`
	RegistryFile    string        `config:"registry_file"`
	ShutdownTimeout time.Duration `config:"shutdown_timeout"`
}

// Validate validates the AalogbeatConfig data and returns an error
// describing all problems or nil if there are none.
func (c AalogbeatConfig) Validate() error {
	// We have default settings for the primary settings,
	// so no need to validate if they are present in the config.
	return nil
}
