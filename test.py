import os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pympler import asizeof


#os.listdir()
#pq.read_table()
#table.to_pandas()
# pa.total_allocated_bytes()
#bytes = []

def pait():
  #bytes.clear()
  fileNames = os.listdir('tmp')
  #fileNames = 5 * fileNames
  for name in fileNames:
    print(name)
    d = ds.dataset('tmp/' + name)
    batches = d.scanner(batch_size=1000).to_batches()
    for b in batches:
      pt = b.to_pandas()
      sz = asizeof.asizeof(pt) + pa.total_allocated_bytes()
      #bytes.append(sz)
      # print(pa.total_allocated_bytes(), asizeof.asizeof(pt), name)
      yield None
    del batches
    del d

def pA():
  myPait = pait()
  print("starting")
  for x in myPait:
    pass

pA()

