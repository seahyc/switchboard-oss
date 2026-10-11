package main

import "flag"

// commandFlagSet keeps the standard library FlagSet behavior while allowing
// options to appear before or after positional arguments, as the usage text
// advertises. FlagSet itself stops at the first positional argument.
type commandFlagSet struct {
	*flag.FlagSet
}

func (fs *commandFlagSet) Parse(args []string) error {
	var options, positionals []string
	endOptions := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			endOptions = true
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if len(arg) < 2 || arg[0] != '-' {
			positionals = append(positionals, arg)
			continue
		}

		options = append(options, arg)
		name, _, hasValue := splitFlag(arg)
		registered := fs.Lookup(name)
		if registered == nil || hasValue || isBoolFlag(registered.Value) {
			continue
		}
		if i+1 < len(args) {
			i++
			options = append(options, args[i])
		} else {
			// Parse the incomplete option set now so a preceding positional
			// cannot be mistaken for this flag's missing value.
			return fs.FlagSet.Parse(options)
		}
	}
	if endOptions {
		options = append(options, "--")
	}
	return fs.FlagSet.Parse(append(options, positionals...))
}

func splitFlag(arg string) (name, value string, hasValue bool) {
	name = arg[1:]
	if len(name) > 0 && name[0] == '-' {
		name = name[1:]
	}
	for i := 0; i < len(name); i++ {
		if name[i] == '=' {
			return name[:i], name[i+1:], true
		}
	}
	return name, "", false
}

func isBoolFlag(value flag.Value) bool {
	boolFlag, ok := value.(interface{ IsBoolFlag() bool })
	return ok && boolFlag.IsBoolFlag()
}
