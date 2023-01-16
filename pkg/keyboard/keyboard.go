package keyboard

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func GetIntegerInput() (int, error) {
	var amount int

	fmt.Print("Enter table rows count ")
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return 0, err
	}

	input = strings.TrimSpace(input)
	amount, err = strconv.Atoi(input)
	if err != nil {
		return 0, err
	}

	if amount <= 0 {
		return 0, errors.New("rows count must be positive")
	}

	return amount, nil
}
