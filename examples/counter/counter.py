#!/usr/bin/env python
 
import sys, copy
sys.path.append('color-py/gen-py')
 
from fuzzylog.ttypes import *
 
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
 
class Counter:
  def __init__(self, client):
    self._client = client 
    self._daghandle = client.new_dag_handle(Colors(numcolors=3, mycolors=[1,2,3]))
    self._dagid = self._daghandle.daghandle

  def inc(self):
    self._client.append(self._dagid,"",0,Colors(numcolors=1, mycolors=[1]),None)

  def dec(self):
    self._client.append(self._dagid,"",0,Colors(numcolors=1, mycolors=[2]),None)

  def reset(self):
    self._client.append(self._dagid,"",0,Colors(numcolors=1, mycolors=[3]),None)
  
  def count(self):
    self._client.snapshot(self._dagid)
    retnode = self._client.get_next(self._dagid)
    incdec = []
    reset = []

    while retnode.errorcode == 1:
      # build DAG
      if retnode.nodecolors.mycolors == [1]:
        incdec.append(1)
      elif retnode.nodecolors.mycolors == [2]:
        incdec.append(-1)
      elif retnode.nodecolors.mycolors == [3]:
        incdec = []
      retnode = self._client.get_next(self._dagid)          

    return sum(incdec)
