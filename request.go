package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

func downloadMFData() {

	url := "https://www.amfiindia.com/modules/NavHistoryAll"
	method := "POST"

	payload := strings.NewReader(`fDate=28-Feb-2025`)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0")
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Accept-Language", "en-US,en;q=0.5")
	req.Header.Add("Accept-Encoding", "gzip, deflate, br, zstd")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Add("X-Requested-With", "XMLHttpRequest")
	req.Header.Add("Origin", "https://www.amfiindia.com")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Referer", "https://www.amfiindia.com/net-asset-value/nav-history")
	req.Header.Add("Sec-Fetch-Dest", "empty")
	req.Header.Add("Sec-Fetch-Mode", "cors")
	req.Header.Add("Sec-Fetch-Site", "same-origin")
	req.Header.Add("Priority", "u=0")
	req.Header.Add("TE", "trailers")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))
}
