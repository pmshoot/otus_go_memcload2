/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package main

import "flag"

// flags
var (
	pattern                string
	idfa, gaid, adid, dvid string
	dry                    bool
)

func init() {
	flag.StringVar(&pattern, "pattern", "data/appsinstalled/*.tsv.gz", "path to data dir (template file name)")
	flag.StringVar(&idfa, "idfa", "127.0.0.1:33013", "idfa memcache server")
	flag.StringVar(&gaid, "gaid", "127.0.0.1:33014", "gaid memcache server")
	flag.StringVar(&adid, "adid", "127.0.0.1:33015", "adid memcache server")
	flag.StringVar(&dvid, "dvid", "127.0.0.1:33016", "dvid memcache server")
	flag.BoolVar(&dry, "dry", false, "dry run")
	flag.Parse()
}

func main() {
	process()
}
