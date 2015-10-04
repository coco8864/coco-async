# ようこそ naru.async Projectへ #
> naru.asyncは、スケーラビリティを志向したjava通信ライブラリです。[Queuelet](http://code.google.com/p/coco-queuelet/)上で動作し以下の要素からできています。
  * IOManager 通信管理
> > nioのchannleを利用して多数の通信回線を少数のThreadで処理
  * StoreManager 通信バッファの管理
> > アプリケーションの処理能力と通信処理能力にはギャップがあります。通信バッファをメモリだけでなく、ファイルにも確保します
  * PoolManager 通信バッファ、オブジェクトのプール管理
> > 多数の通信を同時に行うには、GCを極力させない仕組みが必要です


> ## [Queuelteアプリケーションとしての定義](QueueletDef.md) ##
> ## [Dependency](Dependency.md) ##


