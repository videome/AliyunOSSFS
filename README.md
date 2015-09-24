AliyunOSSFS provides access to the buckets of Aliyun OSS like if they would be separate **readonly** directories via a [FUSE][Linux-Fuse] userland filesystem. This project was inspired by [fuse-jna][fuse-jna] (Java bindings to FUSE) and [JGitFS][JGitFS] (Mount Git repository as FS built on [fuse-jna][fuse-jna]).

## Getting started

#### Grab it

    git clone --recursive git://github.com/videome/AliyunOSSFS

Note: The "--recursive" is necessary to clone necessary submodules as well!

#### Build it and create the distribution files

	cd AliyunOSSFS
	./gradlew installDist

#### Run it

    build/install/AliyunOSSFS/bin/AliyunOSSFS -i <access id of your OSS> -k <access key of your OSS> -e <endpoint of your OSS> -b <the bucket in your OSS> -m <location of mountpoint>
    
or you can specify the credential file of Aliyun OSS for better security. By default, AliyunOSSFS will use the credential file(~/.aliyuncli/osscredentials) created by [Aliyun Offical CLI][Aliyun-cli].

	build/install/AliyunOSSFS/bin/AliyunOSSFS -f <credential file location> -b <the bucket in your OSS> -m <location of mountpoint>

#### Test it
	./gradlew -Doss-key=<access id of your OSS> -Doss-secret=<access key of your OSS> -Doss-endpoint=http://oss-cn-beijing.aliyuncs.com test
   
[Linux-FUSE]: http://fuse.sourceforge.net/
[fuse-jna]: https://github.com/EtiennePerot/fuse-jna
[JGitFS]: https://github.com/centic9/JGitFS
[Aliyun-cli]: https://github.com/aliyun/aliyun-cli