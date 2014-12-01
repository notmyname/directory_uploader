import os
import sys

import swiftclient
import cf_auth


def local_dir_contents_to_swift_obj_name_iter(local_dir_path,
                                              filter_set=None,
                                              space_mapping='_'):
    if not local_dir_path.endswith('/'):
        local_dir_path += '/'
    len_prefix = len(local_dir_path)  # +1 for the slash
    if filter_set is None:
        filter_set = set()
    for dirpath, dirnames, filenames in os.walk(local_dir_path):
        for filename in filenames:
            if filename in filter_set:
                continue
            slash = '' if dirpath.endswith('/') else '/'
            local_name = '%s%s%s' % (dirpath, slash, filename)
            obj_name = local_name[len_prefix:]
            if space_mapping is not None:
                obj_name = obj_name.replace(' ', space_mapping)
            yield local_name, obj_name


def main(target_container, source_dir_path, do_it=True):
    cf_connection = swiftclient.Connection(
        authurl='https://auth.api.rackspacecloud.com/v1.0',
        user=cf_auth.username,
        key=cf_auth.apikey,
        ssl_compression=False,
        retries=2)

    cf_connection.get_auth()

    # will error if the container doesn't exist
    container_headers, full_remote_listing = cf_connection.get_container(
        target_container,
        full_listing=True)

    local_listing = local_dir_contents_to_swift_obj_name_iter(
        source_dir_path, set(['.DS_Store']), space_mapping='_')

    missing_in_remote = set()
    remote_objects = set(x['name'] for x in full_remote_listing)
    for filename, obj_name in local_listing:
        if obj_name not in remote_objects:
            missing_in_remote.add((filename, obj_name))

    len_missing_in_remote = len(missing_in_remote)
    print '%d local files to upload to the container %s' % (
        len_missing_in_remote,
        target_container)

    for i, (local_file, obj_name) in enumerate(missing_in_remote):
        sys.stdout.write('Uploading %s... ' % local_file)
        if not do_it:
            print obj_name
            continue
        sys.stdout.flush()
        with open(local_file) as f:
            # upload the object using a smaller default chunk_size
            # so that it plays better with others
            etag = cf_connection.put_object(
                target_container, obj_name,
                contents=f,
                chunk_size=8192)
        sys.stdout.write(etag + '\n')
        if i % 20 == 0:
            sys.stdout.write('%d remaining to upload\n' % (
                len_missing_in_remote - i))


if __name__ == '__main__':
    try:
        source_dir_path = sys.argv[2]
        target_container = sys.argv[1]
    except IndexError:
        print >>sys.stderr, \
            'Usage: %s target_container path/to/source/dir [-n]' % \
            sys.argv[0]
        sys.exit(1)
    else:
        main(target_container, source_dir_path, do_it=('-n' not in sys.argv))
