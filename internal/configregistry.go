package internal

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type configRegistryT struct {
	parse func( *viper.Viper) (error)
	validate func() ([]error, error)
}

var cfgReg map[string]configRegistryT

func init() {
	cfgReg = make(map[string]configRegistryT)
}


func ConfigRegister(name string, parse func( *viper.Viper) (error), validate func() ([]error, error)) (error) {
	_, found := cfgReg[name]
	if (found) {
		return errors.New("config Section Already Registered")
	}
	cfgReg[name] = configRegistryT{parse: parse, validate: validate}
	return nil
}

func ConfigGetSections() []string {
	var ret []string
	for key := range cfgReg {
		ret = append(ret, key)
	}
	return ret
}

func ConfigCallSection(name string, cfg *viper.Viper) (error) {
	mod, found := cfgReg[name]
	if !found {
		return errors.New("No Config Section Registered")
	}
	return mod.parse(cfg)
}
func ConfigValidateSection(name string) (warnings []error, err error) {
	mod, found := cfgReg[name]
	if !found {
		return nil, nil
	}
	return mod.validate()
}