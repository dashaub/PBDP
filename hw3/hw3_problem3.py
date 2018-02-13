"""


"""

def parse_log_line_w3(line):
    """
    Copied from starter code in cs88_log_parser1.py
    :param line: A string with tab delimiter, three field, and the first field containing
    a timestamp to parse
    """
    tokens = line.strip().split("\t")
    if len(tokens) < 3:
        return None
    else:
        try:
            event_datetime = datetime.strptime(tokens[0], '%Y-%m-%dT%H:%M:%S.%fZ')
            return LogLineW3(event_datetime, tokens[1], tokens[2])
        except ValueError as e:
            try:
                event_datetime = datetime.strptime(tokens[0], '%Y-%m-%dT%H:%M:%SZ')
                return LogLineW3(event_datetime, tokens[1], tokens[2])
            except ValueError as e:
                return None

