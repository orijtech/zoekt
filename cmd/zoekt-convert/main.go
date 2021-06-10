package main

import (
	"fmt"
	"os"

	"github.com/google/zoekt"
)

func main() {
	err := zoekt.ConvertTest(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\t%s\n", os.Args[1], err.Error())
		os.Exit(1)
	}
}
