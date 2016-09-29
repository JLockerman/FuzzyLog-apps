#!/usr/bin/env python
 
import sys, copy
sys.path.append('../../color-py/gen-py')
 
from fuzzylog import colorlayer
from fuzzylog.ttypes import *
from counter import Counter
 
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
 
try:
  # Make socket
  transport = TSocket.TSocket('localhost', 30303)
 
  # Buffering is critical. Raw sockets are very slow
  transport = TTransport.TBufferedTransport(transport)
 
  # Wrap in a protocol
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
 
  # Create a client to use the protocol encoder
  client = colorlayer.Client(protocol)
 
  # Connect!
  transport.open()
 
  c = Counter(client)
  c.inc()
  c.inc()
  c.inc()
  c.inc()
  c.reset()
  c.inc()
  c.dec()
  c.dec()
  c.dec()
  c.dec()
  c.inc()
  c.inc()
  c.inc()
  c.inc()
  c.inc()

  print 'count:{0}'.format(c.count())

  transport.close()
 
except Thrift.TException, tx:
  print "%s" % (tx.message)
