import argparse
import alibabacloud_oss_v2 as oss
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

# 创建命令行参数解析器
parser = argparse.ArgumentParser(description="Download an object from Alibaba Cloud OSS")

# 添加命令行参数 --region，表示存储空间所在的区域，必需参数
parser.add_argument('--region', help='The region in which the bucket is located.', required=True)
# 添加命令行参数 --bucket，表示存储空间的名称；如果 --key 使用 oss://bucket/key 格式，可不传
parser.add_argument('--bucket', help='The name of the bucket.')
# 添加命令行参数 --endpoint，表示其他服务可用来访问OSS的域名，非必需参数
parser.add_argument('--endpoint', help='The domain names that other services can use to access OSS')
# 添加命令行参数 --key，表示对象的名称，必需参数
parser.add_argument('--key', help='The name of the object or prefix.', required=True)
# 添加命令行参数 --output，表示下载到本地的文件或目录路径；不传则使用对象名的文件名
parser.add_argument('--output', help='The local file or directory path to save the downloaded object.')
# 添加命令行参数 --chunk-size，表示分块下载时每个数据块的大小，默认256KB
parser.add_argument('--chunk-size', type=int, default=256 * 1024, help='Download chunk size in bytes.')
# 添加命令行参数 --recursive，表示把 --key 当作文件夹前缀递归下载
parser.add_argument('--recursive', action='store_true', help='Download all objects under the key as a prefix.')


def ensure_credentials():
    """检查 OSS SDK 从环境变量读取凭证时需要的关键信息。"""
    required_envs = [
        'OSS_ACCESS_KEY_ID',
        'OSS_ACCESS_KEY_SECRET',
    ]
    missing_envs = [name for name in required_envs if not os.getenv(name)]

    if missing_envs:
        missing = ', '.join(missing_envs)
        raise RuntimeError(
            f'缺少 OSS 访问凭证环境变量：{missing}。'
            '请先设置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET。'
            '如果使用临时 STS 凭证，还需要设置 OSS_SESSION_TOKEN。'
        )


def parse_bucket_and_key(bucket, key):
    if not key.startswith('oss://'):
        if not bucket:
            raise ValueError('缺少 --bucket。或者你可以把 --key 写成 oss://bucket/object-key 格式。')
        return bucket, key

    parsed = urlparse(key)
    if parsed.scheme != 'oss' or not parsed.netloc or not parsed.path:
        raise ValueError('--key 的 OSS URI 格式不正确，应类似 oss://bucket/path/to/object')

    uri_bucket = parsed.netloc
    object_key = parsed.path.lstrip('/')
    if bucket and bucket != uri_bucket:
        raise ValueError(f'--bucket 是 {bucket}，但 --key URI 中的 bucket 是 {uri_bucket}，两者不一致。')

    return uri_bucket, object_key


def build_output_path(object_key, output):
    filename = Path(object_key).name
    if not filename:
        raise ValueError('无法从 --key 推断本地文件名，请显式传入完整的 --output 文件路径。')

    if output:
        output_path = Path(output).expanduser()
        if output.endswith(os.sep) or output_path.is_dir():
            return output_path / filename
        return output_path

    return Path(filename)


def normalize_prefix(prefix):
    return prefix if prefix.endswith('/') else f'{prefix}/'


def build_recursive_output_path(object_key, prefix, output_dir):
    prefix = normalize_prefix(prefix)
    parent_prefix = prefix.rstrip('/').rsplit('/', 1)[0]
    parent_prefix = f'{parent_prefix}/' if parent_prefix else ''
    relative_key = object_key[len(parent_prefix):] if object_key.startswith(parent_prefix) else object_key
    return Path(output_dir).expanduser() / relative_key


def print_object_info(result):
    print(f'status code: {result.status_code},'
          f' request id: {result.request_id},'
          f' content length: {result.content_length},'
          f' content type: {result.content_type},'
          f' etag: {result.etag},'
          f' last modified: {result.last_modified}')


def download_object(client, bucket, object_key, output_path, chunk_size):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result = client.get_object(oss.GetObjectRequest(
        bucket=bucket,
        key=object_key,
    ))

    total_size = 0
    with result.body as body_stream:
        with output_path.open('wb') as f:
            for chunk in body_stream.iter_bytes(block_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                total_size += len(chunk)

    return total_size


def download_prefix(client, bucket, prefix, output_dir, chunk_size):
    prefix = normalize_prefix(prefix)
    output_dir = Path(output_dir or '.').expanduser()
    paginator = client.list_objects_v2_paginator()
    downloaded_count = 0
    downloaded_size = 0

    for page in paginator.iter_page(oss.ListObjectsV2Request(
            bucket=bucket,
            prefix=prefix,
        )
    ):
        for obj in page.contents:
            if obj.key.endswith('/'):
                continue

            output_path = build_recursive_output_path(obj.key, prefix, output_dir)
            object_size = download_object(client, bucket, obj.key, output_path, chunk_size)
            downloaded_count += 1
            downloaded_size += object_size
            print(f'已下载：{obj.key} -> {output_path} ({object_size} bytes)')

    if downloaded_count == 0:
        raise RuntimeError(f'没有找到前缀为 {prefix} 的对象。请确认 OSS 路径是否正确。')

    print(f'文件夹下载完成：{downloaded_count} 个文件，共 {downloaded_size} bytes，保存到：{output_dir}')

def main():
    # 解析命令行参数
    args = parser.parse_args()

    ensure_credentials()

    # 从环境变量中加载凭证信息，用于身份验证
    credentials_provider = oss.credentials.EnvironmentVariableCredentialsProvider()

    # 加载SDK的默认配置，并设置凭证提供者
    cfg = oss.config.load_default()
    cfg.credentials_provider = credentials_provider

    # 设置配置中的区域信息
    cfg.region = args.region

    # 如果提供了endpoint参数，则设置配置中的endpoint
    if args.endpoint is not None:
        cfg.endpoint = args.endpoint

    # 使用配置好的信息创建OSS客户端
    client = oss.Client(cfg)

    bucket, object_key = parse_bucket_and_key(args.bucket, args.key)
    try:
        if args.recursive:
            download_prefix(client, bucket, object_key, args.output, args.chunk_size)
            return

        output_path = build_output_path(object_key, args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 执行获取对象的请求，指定存储空间名称和对象名称
        result = client.get_object(oss.GetObjectRequest(
            bucket=bucket,
            key=object_key,
        ))

        # 输出获取对象的结果信息，用于检查请求是否成功
        print_object_info(result)

        # 分块写入本地文件，适合下载大文件
        total_size = 0
        with result.body as body_stream:
            with output_path.open('wb') as f:
                for chunk in body_stream.iter_bytes(block_size=args.chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    total_size += len(chunk)

        print(f'文件下载完成：{output_path}，大小：{total_size} bytes')
    except Exception as exc:
        print(f'文件下载失败：{exc}', file=sys.stderr)
        sys.exit(1)

# 当此脚本被直接运行时，调用main函数
if __name__ == "__main__":
    main()  # 脚本入口，当文件被直接运行时调用main函数
