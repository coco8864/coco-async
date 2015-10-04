# queuelet定義ファイル #
> naru.asyncを利用する際のqueuelet定義は以下になります。
```
<?xml version="1.0" encoding="UTF-8"?>
<queueApp checkInterval="check">
  <loader name="main" delegate="false" home="foo">
  </loader>

  <terminal name="timer" threadCount="2" priority="1">
    <queuelet loaderName="main" className="naru.async.timer.TimerManager">
      <param name="timerInterval" value="10"/>
    </queuelet>
  </terminal>

  <terminal name="pool" threadCount="1">
    <queuelet loaderName="main" className="naru.async.pool.PoolManager">
      <param name="recycleInterval" value="10000"/>
      <param name="poolNames" value="p1"/>
      <param name="p1.className" value="naru.async.core.ChannelContext"/>
      <param name="p1.initial" value="128"/>
      <param name="p1.limit" value="12800"/>
      <param name="p1.increment" value="16"/>
      <param name="poolBuffers" value="default"/>
      <param name="default.bufferSize" value="16384"/>
      <param name="default.initial" value="512"/>
      <param name="default.limit" value="102400"/>
      <param name="default.increment" value="128"/>
    </queuelet>
  </terminal>
  
  <terminal name="storeFileWriter" threadCount="4">
    <queuelet loaderName="main" factoryClassName="naru.async.store.StoreManager"
                                         factoryMethodName="getBufferFileWriter">
      <param name="persistenceStore.file" value="persistenceStore.sar"/>
      <param name="page.file" value="page.stp"/>
      <param name="page.readerCount" value="8"/>
      <param name="buffer.0.file" value="buffer.st0"/>
      <param name="buffer.1.file" value="buffer.st1"/>
      <param name="buffer.2.file" value="buffer.st2"/>
      <param name="buffer.3.file" value="buffer.st3"/>
      <param name="buffer.readerCount" value="4"/>
    </queuelet>
  </terminal>
  <terminal name="storeFileReader" threadCount="8">
    <queuelet loaderName="main" factoryClassName="naru.async.store.StoreManager"
                                         factoryMethodName="getBufferFileReader"/>
  </terminal>
  <terminal name="storeDispatcher" threadCount="4">
    <queuelet loaderName="main" factoryClassName="naru.async.store.StoreManager"
                                         factoryMethodName="getStoreDispatcher"/>
  </terminal>

  <terminal name="io" threadCount="4">
    <queuelet loaderName="main" className="naru.async.core.IOManager">
      <param name="selectorCount" value="4"/>
      <param name="selectInterval" value="60000"/>
    </queuelet>
  </terminal>
  <terminal name="dispatch" threadCount="4">
    <queuelet loaderName="main" className="naru.async.core.DispatchManager"/>
  </terminal>
</queueApp>
```