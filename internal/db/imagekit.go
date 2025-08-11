package db

import (
	"fmt"
	"os"

	"github.com/imagekit-developer/imagekit-go"
)

func ConnectImageKit() (*imagekit.ImageKit, error) {

	privateKey := os.Getenv("IMAGEKIT_PRIVATE_KEY")
	publicKey := os.Getenv("IMAGEKIT_PUBLIC_KEY")
	urlEndpoint := os.Getenv("IMAGEKIT_URL_ENDPOINT")

	if privateKey == "" || publicKey == "" || urlEndpoint == "" {
		fmt.Println("Missing required environment variables for ImageKit")
		return nil, nil
	}
	ik := imagekit.NewFromParams(imagekit.NewParams{
		PrivateKey:  privateKey,
		PublicKey:   publicKey,
		UrlEndpoint: urlEndpoint,
	})

	fmt.Println("ImageKit client created")

	return ik, nil
}
