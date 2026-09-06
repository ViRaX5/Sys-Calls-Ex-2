CC = gcc
CFLAGS = -Wall -Wextra -std=gnu11 -O2 -pthread
TARGET = crashRecovery
SRC = crashRecovery.c



all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -f $(TARGET) accounts.txt
