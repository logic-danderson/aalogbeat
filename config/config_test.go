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
	backfillDuration, _ := time.ParseDuration("-1s")

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
		{
			AalogbeatConfig{
				BackfillDuration: backfillDuration,
			},
			"1 error: backfill_duration cannot be negative",
		},
		{
			AalogbeatConfig{
				BackfillStart: "I am not a valid time",
			},
			"1 error: backfill_start has an invalid time value:'I am not a valid time'. The value should be in RFC3339 format.",
		},
		{
			// Can get multiple errors.
			AalogbeatConfig{
				BackfillDuration: backfillDuration,
				BackfillStart:    "Still not valid",
			},
			"2 errors: backfill_duration cannot be negative; backfill_start has an invalid time value:'Still not valid'. The value should be in RFC3339 format.",
		},
	}

	for _, test := range testCases {
		test.run(t)
	}
}

func TestReadConfig(t *testing.T) {
	c := AalogbeatConfig{}
	options := map[string]interface{}{
		"directory":         "some/directory",
		"file_pattern":      "*.txt",
		"batch_size":        500,
		"registry_file":     ".blah.yml",
		"shutdown_timeout":  "20s",
		"backfill_enabled":  true,
		"backfill_start":    "2019-10-02T15:00:00-05:00",
		"backfill_duration": "72h",
	}
	config, err := common.NewConfigFrom(options)
	if err != nil {
		t.Fatal(err)
	}

	if err = ReadConfig(config, &c); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "some/directory", c.Directory, "Directory")
	assert.Equal(t, "*.txt", c.FilePattern, "FilePattern")
	assert.Equal(t, 500, c.BatchSize, "BatchSize")
	assert.Equal(t, ".blah.yml", c.RegistryFile, "RegistryFile")
	dur, _ := time.ParseDuration("20s")
	assert.Equal(t, dur, c.ShutdownTimeout, "ShutdownTimeout")
	assert.Equal(t, true, c.BackfillEnabled, "BackfillEnabled")
	assert.Equal(t, "2019-10-02T15:00:00-05:00", c.BackfillStart, "BackfillStart")
	dur, _ = time.ParseDuration("72h")
	assert.Equal(t, dur, c.BackfillDuration, "BackfillDuration")
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultSettings

	assert.Equal(t, "C:/ProgramData/ArchestrA/LogFiles", c.Directory, "Directory")
	assert.Equal(t, "*.aaLOG", c.FilePattern, "FilePattern")
	assert.Equal(t, 10000, c.BatchSize, "BatchSize")
	assert.Equal(t, ".aalogbeat.yml", c.RegistryFile, "RegistryFile")
	assert.Equal(t, time.Duration(0), c.ShutdownTimeout, "ShutdownTimeout")
	assert.Equal(t, false, c.BackfillEnabled, "BackfillEnabled")
	assert.Equal(t, "", c.BackfillStart, "BackfillStart")
	assert.Equal(t, time.Duration(0), c.BackfillDuration, "BackfillDuration")
}

func newConfig(from map[string]interface{}) *common.Config {
	cfg, err := common.NewConfigFrom(from)
	if err != nil {
		panic(err)
	}
	return cfg
}
