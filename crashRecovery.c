// Submitted by: Amit Lachmann & Noam Eilat

/*
 * crashRecoveryAmit.c - Multi-threaded WAL crash-recovery engine (Exercise 2).
 *
 * Execution flow:
 *   1. Forward pass (main thread, sequential): read the WAL file given as
 *      argv[1] one line at a time, up to and including "=== CRASH ===".
 *      Every UPDATE line's NEW value is applied immediately to an
 *      in-memory page table (SharedDatabase), and also recorded into a
 *      growable log-history array for later Undo. BEGIN/COMMIT/ABORT
 *      lines update a per-transaction state tracker (TxRecord array).
 *   2. Collect every transaction still TX_ACTIVE (never reached COMMIT
 *      or ABORT) once the crash point is reached - these are the ones
 *      that need to be rolled back.
 *   3. Parallel Undo: one pthread per still-ACTIVE transaction. Each
 *      thread scans the *shared* log-history array from the end
 *      backward, skipping every entry that isn't its own transaction,
 *      restoring the OLD value for each entry that is.
 *   4. Join every worker, sort the final page table alphabetically by
 *      key, and write accounts.txt as "KEY: VALUE" lines.
 *
 * Design note (documented assumption, not a bug): per the assignment
 * spec, recovery only undoes transactions that are ACTIVE at crash time.
 * A transaction that reached ABORT before the crash is not touched here -
 * whatever NEW value it last wrote during the forward pass simply stands.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#define MAX_LINE_LEN 256
#define INITIAL_CAPACITY 32

typedef enum
{
    TX_INACTIVE,
    TX_ACTIVE,
    TX_COMMITTED,
    TX_ABORTED
} TxState;

typedef struct
{
    char key[64];
    int value;
} Page;

typedef struct
{
    Page pages[1000];
    int page_count;
    pthread_mutex_t lock;
} SharedDatabase;

typedef struct
{
    int tx_id;
    char key[64];
    int old_val;
    int new_val;
} LogEntry;

typedef struct
{
    int target_tx_id;
    LogEntry *log_history;
    int log_size;
    SharedDatabase *db;
} ThreadArgs;

typedef struct
{
    int tx_id;
    TxState state;
} TxRecord;

int findRelevantPage(char *key, SharedDatabase *db);
void *runThread(void *argsIn);
int read_until_newline(int fd, char *buffer, int max_len);
int comparePages(const void *a, const void *b);

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        fprintf(stderr, "Invalid run params\n");
        return 1;
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd == -1)
    {
        perror("couldn't open read file");
        return EXIT_FAILURE;
    }

    SharedDatabase db;
    db.page_count = 0;
    if (pthread_mutex_init(&db.lock, NULL) != 0)
    {
        perror("Failed to initialize mutex");
        close(fd);
        return 1;
    }

    int logs_cap = INITIAL_CAPACITY;
    LogEntry *logs = (LogEntry *)malloc(sizeof(LogEntry) * logs_cap);
    int log_count = 0;

    int records_cap = INITIAL_CAPACITY;
    int records_count = 0;
    TxRecord *records = (TxRecord *)malloc(sizeof(TxRecord) * records_cap);

    char buffer[MAX_LINE_LEN];

    while (1)
    {
        int bytes = read_until_newline(fd, buffer, MAX_LINE_LEN);
        if (bytes <= 0)
        {
            break;
        }

        buffer[strcspn(buffer, "\r\n")] = 0;
        if (strlen(buffer) == 0)
            continue;

        char *subString = strtok(buffer, " \t\r\n");
        if (subString == NULL)
            continue;

        if (strcmp(subString, "===") == 0)
        {
            char *next = strtok(NULL, " \t\r\n");
            if (next != NULL && strcmp(next, "CRASH") == 0)
            {
                break; /* stop reading immediately - nothing after this line is ever processed */
            }
            continue;
        }

        if (strcmp(subString, "TX") != 0)
        {
            continue;
        }

        subString = strtok(NULL, " \t\r\n");
        if (subString == NULL)
            continue;
        int id = atoi(subString);

        /* Find this transaction's existing TxRecord (a tx_id can appear
         * across several lines - BEGIN, UPDATE, COMMIT/ABORT - and all of
         * them must update the SAME record, not create a new one each
         * time). Linear search is fine at this assignment's scale. */
        int rec_idx = -1;
        for (int i = 0; i < records_count; i++)
        {
            if (records[i].tx_id == id)
            {
                rec_idx = i;
                break;
            }
        }

        if (rec_idx == -1)
        {
            /* First time this tx_id has been seen - append a new record,
             * growing the array (doubling capacity) if it's full. */
            if (records_count >= records_cap)
            {
                records_cap *= 2;
                TxRecord *temp = realloc(records, sizeof(TxRecord) * records_cap);
                if (!temp)
                {
                    perror("realloc failed");
                    break;
                }
                records = temp;
            }
            rec_idx = records_count++;
            records[rec_idx].tx_id = id;
            records[rec_idx].state = TX_INACTIVE;
        }

        subString = strtok(NULL, " \t\r\n");
        if (subString == NULL)
            continue;

        if (strcmp(subString, "BEGIN") == 0)
        {
            records[rec_idx].state = TX_ACTIVE;
        }
        else if (strcmp(subString, "COMMIT") == 0)
        {
            records[rec_idx].state = TX_COMMITTED;
        }
        else if (strcmp(subString, "ABORT") == 0)
        {
            records[rec_idx].state = TX_ABORTED;
        }
        else if (strcmp(subString, "UPDATE") == 0)
        {
            char *key_str = strtok(NULL, " \t\r\n");
            /* "OLD:100" and "NEW:150" are each a single whitespace-
             * delimited token (no space after the colon), so each is
             * read as one token here and then split into its label and
             * numeric value together via sscanf - which also validates
             * that the label actually reads "OLD"/"NEW". */
            char *old_token = strtok(NULL, " \t\r\n");
            char *new_token = strtok(NULL, " \t\r\n");

            int oldVal = 0;
            int newVal = 0;
            int old_ok = (old_token != NULL) && (sscanf(old_token, "OLD:%d", &oldVal) == 1);
            int new_ok = (new_token != NULL) && (sscanf(new_token, "NEW:%d", &newVal) == 1);

            if (key_str && old_ok && new_ok)
            {
                int pageNumber = findRelevantPage(key_str, &db);
                if (pageNumber == -1)
                {
                    if (db.page_count < 1000)
                    {
                        pageNumber = db.page_count++;
                        /* snprintf bounds the copy to this field's actual size and
                         * always NUL-terminates - key_str comes from strtok() with
                         * no length limit of its own, so this guards against an
                         * unexpectedly long key overflowing the 64-byte field. */
                        snprintf(db.pages[pageNumber].key, sizeof(db.pages[pageNumber].key), "%s", key_str);
                        db.pages[pageNumber].value = newVal;
                    }
                }
                else
                {
                    db.pages[pageNumber].value = newVal;
                }

                if (log_count >= logs_cap)
                {
                    logs_cap *= 2;
                    LogEntry *temp_logs = realloc(logs, sizeof(LogEntry) * logs_cap);
                    if (!temp_logs)
                    {
                        perror("realloc logs failed");
                        break;
                    }
                    logs = temp_logs;
                }

                logs[log_count].tx_id = id;
                snprintf(logs[log_count].key, sizeof(logs[log_count].key), "%s", key_str);
                logs[log_count].old_val = oldVal;
                logs[log_count].new_val = newVal;
                log_count++;
            }
        }
    }
    close(fd);

    int thread_count = 0;
    for (int i = 0; i < records_count; i++)
    {
        if (records[i].state == TX_ACTIVE)
        {
            thread_count++;
        }
    }

    pthread_t *threads = NULL;
    if (thread_count > 0)
    {
        threads = (pthread_t *)malloc(sizeof(pthread_t) * thread_count);
        if (threads == NULL)
        {
            perror("Failed to allocate threads array");
            free(logs);
            free(records);
            pthread_mutex_destroy(&db.lock);
            return 1;
        }

        int thread_index = 0;
        for (int i = 0; i < records_count; i++)
        {
            if (records[i].state == TX_ACTIVE)
            {
                ThreadArgs *arg = malloc(sizeof(ThreadArgs));
                if (arg == NULL)
                {
                    perror("Failed to allocate thread args");
                    break;
                }

                arg->target_tx_id = records[i].tx_id;
                arg->log_history = logs;
                arg->log_size = log_count;
                arg->db = &db;

                if (pthread_create(&threads[thread_index], NULL, runThread, arg) != 0)
                {
                    perror("Failed to create thread");
                    free(arg);
                    break;
                }
                thread_index++;
            }
        }

        for (int i = 0; i < thread_index; i++)
        {
            pthread_join(threads[i], NULL);
        }
        free(threads);
    }

    /* Sort alphabetically by key - only safe here because every worker
     * thread has already been joined above, so nothing can still be
     * concurrently modifying db.pages. */
    qsort(db.pages, db.page_count, sizeof(Page), comparePages);

    FILE *outfile = fopen("accounts.txt", "w");
    if (outfile == NULL)
    {
        perror("Failed to open accounts.txt for writing");
    }
    else
    {
        /* Output format per spec: "KEY: VALUE", one page per line. */
        for (int i = 0; i < db.page_count; i++)
        {
            fprintf(outfile, "%s: %d\n", db.pages[i].key, db.pages[i].value);
        }
        fclose(outfile);
    }

    free(logs);
    free(records);
    pthread_mutex_destroy(&db.lock);

    return 0;
}

/*
 * Function: read_until_newline
 * -------------------------------
 * Purpose: Reads one line from an open file descriptor, one byte at a
 *          time, stopping at '\n' or EOF or when the buffer is full.
 * Arguments: fd      - an already-open, readable file descriptor.
 *            buffer  - destination buffer for the line (including the
 *                       trailing '\n' if one was found).
 *            max_len - total capacity of buffer, including room for the
 *                       NUL terminator this function always writes.
 * Returns: The number of bytes written into buffer (0 at EOF with
 *          nothing read, -1 if read() failed).
 * Assumptions: buffer is at least 1 byte; fd is valid. Does not retry on
 *              EINTR - a read() interrupted by a signal is treated the
 *              same as a hard error.
 */
int read_until_newline(int fd, char *buffer, int max_len)
{
    int total_read = 0;
    char ch;
    int num_read;

    if (max_len == 0 || buffer == NULL)
        return 0;

    while (total_read < max_len - 1)
    {
        num_read = read(fd, &ch, 1);
        if (num_read < 0)
        {
            return -1;
        }
        else if (num_read == 0)
        {
            break;
        }

        buffer[total_read++] = ch;
        if (ch == '\n')
        {
            break;
        }
    }
    buffer[total_read] = '\0';
    return total_read;
}

/*
 * Function: findRelevantPage
 * ------------------------------
 * Purpose: Looks up a page's index in the shared database by key, via a
 *          linear scan. Used both by the single-threaded forward pass
 *          (building the table) and by each worker thread during Undo.
 * Arguments: key - the NUL-terminated key to search for.
 *            db  - the SharedDatabase to search.
 * Returns: The index into db->pages of the matching entry, or -1 if no
 *          page with that key exists.
 * Assumptions: Only reads db->page_count and db->pages[i].key, never
 *              db->pages[i].value - both are fully finalized during the
 *              single-threaded forward pass and never written again once
 *              a worker thread starts, which is why runThread() below
 *              can safely call this from inside its own critical section
 *              without this function needing any locking of its own.
 */
int findRelevantPage(char *key, SharedDatabase *db)
{
    for (int i = 0; i < db->page_count; i++)
    {
        if (strcmp(key, db->pages[i].key) == 0)
        {
            return i;
        }
    }
    return -1;
}

/*
 * Function: comparePages
 * --------------------------
 * Purpose: qsort() comparator ordering Page entries alphabetically by key.
 * Arguments: a, b - void* pointers to two Page elements (as required by
 *                   qsort()'s comparator signature).
 * Returns: <0, 0, or >0, matching strcmp()'s contract on the two keys.
 * Assumptions: a and b both point to valid Page structs.
 */
int comparePages(const void *a, const void *b)
{
    Page *p1 = (Page *)a;
    Page *p2 = (Page *)b;
    return strcmp(p1->key, p2->key);
}

/*
 * Function: runThread
 * -----------------------
 * Purpose: pthread entry point. Rolls back exactly one transaction
 *          (args->target_tx_id) by scanning the shared log history from
 *          the end backward and restoring the OLD value of every entry
 *          that belongs to this transaction.
 * Arguments: argsIn - a (void *) cast of a ThreadArgs*, heap-allocated by
 *                     the caller (main thread) specifically for this
 *                     thread; this function takes ownership of it and
 *                     frees it before returning.
 * Returns: NULL always.
 * Assumptions: log_history/log_size are the SAME shared array/length for
 *              every worker thread (each thread filters for its own
 *              tx_id) - only ever read here, never written, so no lock
 *              is needed to access it. db->pages/page_count/lock ARE
 *              shared and mutated, so every access to them - including
 *              the findRelevantPage() lookup, not just the final write -
 *              is done while holding db->lock.
 */
void *runThread(void *argsIn)
{
    ThreadArgs *args = (ThreadArgs *)argsIn;
    int pageNumber;

    for (int i = args->log_size - 1; i >= 0; i--)
    {
        if (args->log_history[i].tx_id == args->target_tx_id)
        {
            /*
             * Critical section. Race condition being prevented: every
             * worker thread runs this same "find page by key, then
             * overwrite its value" sequence concurrently, against the
             * SAME db->pages array. Without this lock, two threads could
             * interleave a read and a write to the same Page (a lost
             * update) if two active transactions happened to touch the
             * same key. Locking around the lookup and the write together
             * makes the whole sequence one atomic, indivisible step, so
             * only one thread is ever inside it at a time.
             */
            int lock_rc = pthread_mutex_lock(&args->db->lock);
            if (lock_rc != 0)
            {
                fprintf(stderr, "Error: pthread_mutex_lock failed for tx %d: %s\n",
                        args->target_tx_id, strerror(lock_rc));
                free(args);
                return NULL;
            }

            pageNumber = findRelevantPage(args->log_history[i].key, args->db);
            if (pageNumber == -1)
            {
                /* Should be impossible: every key that ever appears in an
                 * UPDATE line is registered into db->pages during the
                 * forward pass, before any worker thread starts. */
                fprintf(stderr, "Error finding the relevant page\n");
            }
            else
            {
                args->db->pages[pageNumber].value = args->log_history[i].old_val;
            }

            int unlock_rc = pthread_mutex_unlock(&args->db->lock);
            if (unlock_rc != 0)
            {
                fprintf(stderr, "Error: pthread_mutex_unlock failed for tx %d: %s\n",
                        args->target_tx_id, strerror(unlock_rc));
                free(args);
                return NULL;
            }
        }
    }

    free(args);
    return NULL;
}
