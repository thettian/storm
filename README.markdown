Storm is a distributed realtime computation system. Similar to how Hadoop provides a set of general primitives for doing batch processing, Storm provides a set of general primitives for doing realtime computation. Storm is simple, can be used with any programming language, [is used by many companies](https://github.com/nathanmarz/storm/wiki/Powered-By), and is a lot of fun to use!

The [Rationale page](https://github.com/nathanmarz/storm/wiki/Rationale) on the wiki explains what Storm is and why it was built. [This presentation](http://vimeo.com/40972420) is also a good introduction to the project.

Storm has a website at [storm-project.net](http://storm-project.net). Follow [@stormprocessor](https://twitter.com/stormprocessor) on Twitter for updates on the project.

## DWEB

* 参照Storm DRPC 的实现，对Storm进行获取Web HTTP请求的扩展，开发一种新架构的的Web服务器集群。
* 将Web服务器Web HTTP请求过程划分为请求接收和请求处理两个部分。请求接收使用Restlet技术，主要负责接收来自Web客户端的请求与返回响应结果的功能；请求处理过程转交给Storm集群进行处理。
* Web HTTP请求处理过程借用Storm的Stream处理模型，取代过去的函数调用、返回结果的方式，更加符合实时流处理的过程。

## Documentation

Documentation and tutorials can be found on the [Storm wiki](http://github.com/nathanmarz/storm/wiki).

## License

The use and distribution terms for this software are covered by the
Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
which can be found in the file LICENSE.html at the root of this distribution.
By using this software in any fashion, you are agreeing to be bound by
the terms of this license.
You must not remove this notice, or any other, from this software.

## Project lead

* Nathan Marz ([@nathanmarz](http://twitter.com/nathanmarz))

## Core contributors

* James Xu ([@xumingming](https://github.com/xumingming))
* Jason Jackson ([@jason_j](http://twitter.com/jason_j))
* Andy Feng ([@anfeng](https://github.com/anfeng))

## DWEB contributor

* thettian ([@thettian](https://github.com/thettian))
