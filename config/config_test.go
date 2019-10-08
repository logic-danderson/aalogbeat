// +build !integration

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/beats/libbeat/common"
)

type validationTestCase struct {
	config AalogbeatConfig
	errMsg string
}

func (v validationTestCase) run(t *testing.T) {
	if v.errMsg == "" {
		assert.NoError(t, v.config.Validate())
	} else {
		err := v.config.Validate()
		if err != nil {
			assert.Contains(t, err.Error(), v.errMsg)
		} else {
			t.Errorf("exected error with '%s'", v.errMsg)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	testCases := []validationTestCase{
		// Top-level config
		{
			AalogbeatConfig{
				Directory:   "the/directory",
				FilePattern: "*.aaLOG",
			},
			"", // No error
		},
		{
			AalogbeatConfig{},
			"", // No error
		},
	}

	for _, test := range testCases {
		test.run(t)
	}
}

func TestReadConfig(t *testing.T) {
	c := AalogbeatConfig{}
	options := map[string]interface{}{
		"directory":        "some/directory",
		"file_pattern":     "*.txt",
		"batch_size":       500,
		"registry_file":    ".blah.yml",
		"shutdown_timeout": "20s",
	}
	config, err := common.NewConfigFrom(options)
	if err != nil {
		t.Fatal(err)
	}
	err = ReadConfig(config, &c)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "some/directory", c.Directory, "Directory")
	assert.Equal(t, "*.txt", c.FilePattern, "FilePattern")
	assert.Equal(t, 500, c.BatchSize, "BatchSize")
	assert.Equal(t, ".blah.yml", c.RegistryFile, "RegistryFile")
	dur, err := time.ParseDuration("20s")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, dur, c.ShutdownTimeout, "ShutdownTimeout")
}

func newConfig(from map[string]interface{}) *common.Config {
	cfg, err := common.NewConfigFrom(from)
	if err != nil {
		panic(err)
	}
	return cfg
}
