package cmd

import (
//	"fmt"


	"github.com/Fishwaldo/restic-nats-server/internal"
	"github.com/spf13/viper"
)


func VerifyConfig() (error) {
	var warnings []error
	sections := internal.ConfigGetSections()
	for _, mod := range sections {
		warn, err := internal.ConfigValidateSection(mod)
		if err != nil {
			internal.Log.Fatal("Config Error: %s", err)
			return err
		}
		if len(warn) > 0 {
			warnings = append(warnings, warn...)
		}
	}
	for _, warn := range warnings {
		internal.Log.Warn("Config Warning: %s", warn)
	}
	return nil
}

func LoadConfig() (error) {
	/* do our Top Level Config First */


	/* then our Modules Config */
	sections := internal.ConfigGetSections()
	for _, mod := range sections {
		cfg := viper.Sub(mod)
		if cfg != nil {
			if err := internal.ConfigCallSection(mod, cfg); err != nil {
				internal.Log.Warn("Module %s Config Error: %s", mod, err)
			}
		} else {
			internal.Log.Warn("No Config Section for Module %s", mod)
		}
	}

	return VerifyConfig()
}