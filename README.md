Correctness
-----------

To check for correct, a combination of unit tests and tests against generated sample data is done.

Unit tests check various situations that can occur, such as:

- Transaction crediting and debiting balances
- Transaction state changes due to disputes
- Validity of transaction state changes
- Whether locked account prevent further action
- Whether reference transaction exists
- Clients can only dispute their own transactions

Sample data is generated with generate.py Python script. It will create a large file consisting of deposit and
withdrawal events. It serves to check logic of deposit/withdrawal transactions and benchmark performance on large
datasets.

Example: ``python3 generate.py --clients 10 --transactions 1000000``

Safety and Robustness
---------------------

- Invalid transactions are ignored
- Errors during reading and writing CSV files are propagated to the main, which prints the error.

Efficiency
----------

Can you stream values through memory as opposed to loading the entire data set upfront? 
- CSV data is read in chunks (not whole file at once) and sent for processing via a channel.
- It runs in a dedicated thread and is logically separate from the transaction processing.
- Another thread receives the transaction and processes it.
- Thinking further, we could have multiple workers/threads, each responsible for some clients.

What if your code was bundled in a server, and these CSVs came from thousands of concurrent TCP streams?
- The TCP server and the connections can run in dedicated threads and/or tasks.
- They can then pass the received transactions to a channel for further processing.
- In this case, async implementation would make more sense, as it would scale better.
