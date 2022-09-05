package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"

	"github.com/jaswdr/faker"
)

func main() {
	f := faker.New()

	file, err := os.OpenFile("data.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal(err)
	}

	keyFile, err := os.OpenFile("data_keys.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()
	defer keyFile.Close()

	for i := 0; i < 100000; i++ {
		key := f.Person().FirstName()

		_, err = file.WriteString(fmt.Sprintf("%s|%s\n", key, f.RandomStringWithLength(rand.Intn(10000))))
		if err != nil {
			log.Fatal(err)
		}

		_, err = keyFile.WriteString(fmt.Sprintf("%s\n", key))
		if err != nil {
			log.Fatal(err)
		}
	}
}
