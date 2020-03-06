package commands

import (
	"fmt"
	"github.com/spf13/pflag"
	"strings"
)

const RequiredOption = "REQUIRED_OPTION"

func CheckRequiredFlags(commandFlags *pflag.FlagSet) error {
	var missingFlagNames []string
	commandFlags.VisitAll(func(flag *pflag.Flag) {
		requiredAnnotation, found := flag.Annotations[RequiredOption]
		if !found {
			return
		}
		if (requiredAnnotation[0] == "true") && !flag.Changed {
			missingFlagNames = append(missingFlagNames, flag.Name)
		}
	})
	if len(missingFlagNames) > 0 {
		return fmt.Errorf(`required flag(s) "%s" not set`, strings.Join(missingFlagNames, `", "`))
	}
	return nil
}
