# TO DO


## Ide: Buat keberjalanan integrasi, pake multiple granularity: jadi ada lock di level table

1. Dengan mempertimbangkan Query Processor harus baca semua primary key yang terkena efek dari transaksi, berarti harus ada lock di level tabel juga buat ngehandle hal tersebut (buat cari row apa aja yand didapetin dari query WHERE -> berarti perlu membaca semua row pada suatu tabel).

2. Level granularity bisa juga sampe level Cell (atribut tertentu dari suatu row)
    Cell:
        self.tabel
        self.pkey_row
        self.attribute


## Tambah attribute di class Response

1. Tambah attribute status: str (kalo allowed nya False, status nya bisa wait atau abort)


## Rombak mekanisme Lock di class ConcurrencyControlManager

1. Ubah struktur atribut activeLock jadi map:
    Lock_S = {Row: [...transaction_id]}
    Lock_X = {Row: transaction_id}

2. Hapus activeLocks

3. Tambah variabel transactionLock: {transaction_id: [...Row]}


## Tambah class Transaction

1. Attribute:
    - self.transaction_id
    - self.action
    - self.level
    - self.dataItem (nama tabel/Row/Cell)


## Mekanisme Queue/Wait  di class ConcurrencyControlManager

1. Tambah attribute pendingTransaction: [...transaction_id]

2. Tambah attribute waitingList: [...Transaction]

3. Setiap melakukan commit, maka:
    - Cek waitingList kosong atau engga:
        - Kalau ga kosong, execute yang di waiting list dulu:
            - Cek dari awal apakah bisa di execute atau harus nge-wait transaksi yang lain buat commit (cara cek bisa execute ato engga nya liat dari wait-for-graph).
            - Kalo bisa execute berarti execute terus pop dari waitingList
            - Kalo gabisa, berarti skip aja next ke yang waitingList berikutnya
            - Kalo selama nge-iterasi di waitingList ada yang commit, nextnya coba execute transaksi di waitingList dari awal lagi kalo ada.
        - Kalau kosong, lanjut execute urutan biasa
    

## Detect Deadlock

1. Bikin class WaitForGraph:
    - Attribute:
    self.graph: {tid_waiting: tid_waited} (tid_waiting -> tid_waited)

    - Method:
    def addEdge(self, tid_waiting, tid_waited)
    def deleteEdge(self, tid_waiting, tid_waited)
    def deleteNode(self, tid) -> nge-loop self.deleteEdge(any, tid)
    def isCyclic(self)