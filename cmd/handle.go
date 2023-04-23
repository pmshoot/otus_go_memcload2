package main

import (
	"fmt"
	pb "github/pmshoot/otus_go_memcload2/internal/pkg/memcload2pb"
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"google.golang.org/protobuf/proto"
)

const (
	normal_error_rate = 0.01
)

// memlient структура для хранения данных memcache
type memClient struct {
	Address *string          // адрес сервера
	Client  *memcache.Client // ссылка на объект сервера
}

// appInstalled промежуточная структура данных для передачи сырых данных из файла
// для подготовки protobuf message
type appInstalled struct {
	DevType string
	DevId   string
	Lat     *float64
	Lon     *float64
	Apps    []uint32
}

// process читает и парсит данные из gzip файлов,
// формирует protobuf message и отправляет в memcache
func process() {
	var (
		errors, processed float32
		// devices_memc      map[string]*memClient
	)
	errors, processed = 0, 0 // счетчики ошибок и успешно обработанных записей

	devices_memc := map[string]*memClient{
		"idfa": {Address: &idfa, Client: memcache.New(idfa)},
		"gaid": {Address: &gaid, Client: memcache.New(gaid)},
		"adid": {Address: &adid, Client: memcache.New(adid)},
		"dvid": {Address: &dvid, Client: memcache.New(dvid)},
	}

	flist, _ := filepath.Glob(pattern)
	if len(flist) == 0 {
		log.Fatalln("Не найдены подходящие файлы")
	}

	readGzipDataCh := make(chan []byte)
	go readGzipFile(flist, readGzipDataCh)

	for row := range readGzipDataCh {
		appInstalled, err := parseAppInstalled(string(row))
		if err != nil {
			log.Println(err.Error())
			errors++
			continue
		}

		ua := &pb.UserApps{} // protobuf message data object
		ua.Lat = appInstalled.Lat
		ua.Lon = appInstalled.Lon
		ua.Apps = appInstalled.Apps
		key := fmt.Sprintf("%v:%v", appInstalled.DevType, appInstalled.DevId)
		packed, err := proto.Marshal(ua) // serialized data
		if err != nil {
			log.Println(err.Error())
			errors++
			continue
		}

		memc, exists := devices_memc[appInstalled.DevType]
		if !exists {
			log.Printf("Unknown device type: %v", appInstalled.DevType)
			errors++
			continue
		}

		// send serialized data to memcache
		if err := insertAppinstalled(memc, &key, packed); err == nil {
			processed++
		} else {
			log.Print(err.Error())
			errors++
		}

	}

	if errors == 0 && processed == 0 {
		return // nothing is done
	}
	error_rate := errors / processed
	if error_rate < normal_error_rate {
		log.Printf("Acceptable error rate (%v). Successfull load\n", error_rate)
	} else {
		log.Printf("High error rate (%v > %v). Failed load\n", error_rate, normal_error_rate)
	}

	log.Println("End up")
}

// parseAppInstalled парсит сырые данные из файла и формирует структуру
// для дальнейшего формирования protobuf message
func parseAppInstalled(row string) (*appInstalled, error) {
	line := strings.Split(row, "\t")
	if len(line) != 5 {
		return nil, fmt.Errorf("expected 5 octets: %s", line) // must be 5 octets
	}

	devType, devId := line[0], line[1]
	if devType == "" || devId == "" {
		return nil, fmt.Errorf("expected string: devType=%v devID=%v", devType, devId)
	}
	lat, err := strconv.ParseFloat(line[2], 64)
	if err != nil {
		return nil, fmt.Errorf("convert Lat to float failed")
	}
	lon, err := strconv.ParseFloat(line[3], 64)
	if err != nil {
		return nil, fmt.Errorf("convert Lon to float failed")
	}
	apps := line[4]
	rawAppsList := strings.Split(apps, ",")
	intAppsList := []uint32{}

	for _, appString := range rawAppsList {
		appCode, err := strconv.ParseUint(appString, 10, 32)
		if err == nil {
			intAppsList = append(intAppsList, uint32(appCode)) // формирование списка int кодов из строк
		}
	}

	if len(rawAppsList) != len(intAppsList) {
		log.Printf("Not all user apps are digits: '%v'\n", apps)
	}

	appInstalled := appInstalled{
		DevType: devType,
		DevId:   devId,
		Lat:     &lat,
		Lon:     &lon,
		Apps:    intAppsList,
	}

	return &appInstalled, nil

}

// insertAppinstalled отправляет данные в memcache
func insertAppinstalled(memc *memClient, key *string, message []byte) error {
	if dry {
		log.Printf("%v --> %s:%v\n", *memc.Address, *key, message) // проверка формируемых данных
	} else {
		if err := memc.Client.Set(&memcache.Item{Key: *key, Value: message}); err != nil {
			return err
		}
	}
	return nil
}
