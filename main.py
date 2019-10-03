import argparse
import csv2cmdb as c
import re
import box


def ingest(args):
    """Upload the elements and connections to Freshservice"""
    c.add_update_assets(args.elemfile, filetype(args.elemfile))
    c.add_rela(args.relfile, args.elemfile, filetype(args.elemfile))
    # upload the files to the archive
    box.box_upload_elements(args.elemfile)
    box.box_upload_relations(args.relfile)


def delete(args):
    """Delete the elements and connections from Freshservice"""
    c.mass_delete(args.elemfile,filetype(args.elemfile))


def filetype(str):
    """Extract the filetype from string by using regex

    :param str: string that contains the full name or path of the file
    :return: str
    """
    return re.findall('[^.]*$', str)[0]


if __name__ == "__main__":

    main_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    main_parser.add_argument("--elemfile", "-e", default=None, help='element file')
    main_parser.add_argument("--relfile", "-r", default=None, help='relationships file')

    subparsers = main_parser.add_subparsers(title="subcommands",
                                            description='valid subcommands',
                                            help='additional help')

    ingest_parser = subparsers.add_parser('ingest', description=ingest.__doc__)
    ingest_parser.set_defaults(func=ingest)

    delete_parser = subparsers.add_parser('delete', description=delete.__doc__)
    delete_parser.set_defaults(func=delete)

    args = main_parser.parse_args()

    try:
        print(args.func)
    except AttributeError: # there are no subfunctions
        main_parser.print_help()
        exit(1)

    # fix timestamp
    args.elemfile = args.elemfile.replace('\\', '')
    args.relfile = args.relfile.replace('\\', '')

    if not args.elemfile or not args.relfile:
        print('Please provide both the elements and relationships file')
        exit(1)

    args.func(args)