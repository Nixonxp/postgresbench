package keyboard

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func GetIntegerInput(message string) (int, error) {
	var inputInteger int

	fmt.Print(message)
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return 0, err
	}

	input = strings.TrimSpace(input)
	inputInteger, err = strconv.Atoi(input)
	if err != nil {
		return 0, err
	}

	if inputInteger <= 0 {
		return 0, errors.New("input digit must be positive")
	}

	return inputInteger, nil
}
