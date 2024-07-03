import pprint
import json
from copy import deepcopy


def strip_password_for_reporting(content):
    """
        Don't log passwords! Strip out password values from `content`.
        Because this might cause side effects we use deepcopy.

        :param content: dict, content maybe containing a password
        :return: content without password
    """
    if not content:
        # if there is no content, e.g. empty string, then nothing to strip!
        #logger.info("No password to strip.")
        return content
    elif not content.get('password'):
        return content
    else:
        content_for_reporting = deepcopy(content)
        replacement_text = 'password removed'
        content_for_reporting['password'] = replacement_text
        return content_for_reporting


def plog(content):
    """
        Format json content for pretty printing to the logger.

        If `content` is not json, try to identify what it is and then try
        to format it appropriately.

        :param content: assumed to be json
        :return formatted_content:
    """
    # be safe, strip the passwords for dicts
    if isinstance(content, dict):
        content = strip_password_for_reporting(content)

    # set a default pass-through value of an empty string in order
    # to catch None, because if a calling method tries to write()
    # the output of plog() will error out in the attempt.
    formatted_content = ''
    try:
        formatted_content = json.dumps(content, indent=4, sort_keys=True)
    except (ValueError, TypeError):
        # oops, this wasn't actually json

        # try pretty-printing based on the guessed content type
        from requests.structures import CaseInsensitiveDict
        import deepdiff

        if isinstance(content, dict):
            formatted_content = pprint.pformat(content, indent=1, width=100)
        elif isinstance(content, list):
            formatted_content = pprint.pformat(content, indent=1, width=100)
        elif isinstance(content, CaseInsensitiveDict):
            formatted_content = pprint.pformat(dict(content), indent=1, width=100)
        elif isinstance(content, deepdiff.diff.DeepDiff):
            formatted_content = pprint.pformat(content, indent=1, width=160, depth=4)
        elif isinstance(content, bytes):
            # is this XML?
            if content[:5] == b'<?xml':
                import xml.dom.minidom
                xml = xml.dom.minidom.parseString(content)
                formatted_content = str(xml.toprettyxml())
            else:
                # no, this is some other kind of byte string
                msg = f"Unable to pretty print the content that starts with {content[:20]}"
                print(msg)
                pass

    return formatted_content
