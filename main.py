import argparse
import logging
import os
import pprint
import sys

import httpx

DEFAULT_API_URL = 'https://repository.library.brown.edu/api/search/'
DEFAULT_QUERY = '-object_type:bdr-collection'
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_TOP_N = 10
DEFAULT_RESOURCE_TYPE_FIELD = 'resource_type_ssi'
DEFAULT_OBJECT_TYPE_FIELD = 'object_type'
MEDIA_OBJECT_TYPES = ['pdf', 'image', 'audio', 'video']

logging.basicConfig(
    level=logging.DEBUG if os.getenv('LOG_LEVEL') == 'DEBUG' else logging.INFO,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S',
    # filename=log_file_path,
)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """
    Builds the command-line parser.

    Called by: main()
    """
    parser = argparse.ArgumentParser(
        description='Returns the total public item count in the BDR plus rough Solr facet breakdowns.'
    )
    parser.add_argument(
        '--api-url',
        default=DEFAULT_API_URL,
        help='BDR search API URL. Default: %(default)s',
    )
    parser.add_argument(
        '--query',
        default=DEFAULT_QUERY,
        help='Solr query used to define the BDR items being counted. Default: %(default)s',
    )
    parser.add_argument(
        '--timeout',
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help='HTTP timeout in seconds. Default: %(default)s',
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=DEFAULT_TOP_N,
        help='Number of top facet values to show for the rough breakdown. Default: %(default)s',
    )
    return parser


def build_request_url(api_url: str, params: dict[str, str | int]) -> str:
    """
    Builds a browser-ready GET URL from the API URL and request params.

    Called by: fetch_search_payload()
    """
    return str(httpx.URL(api_url).copy_merge_params(params))


def fetch_search_payload(api_url: str, params: dict[str, str | int], timeout: float) -> dict:
    """
    Fetches JSON data from the public BDR search API.

    Called by: fetch_total_count()
    Called by: fetch_facet_counts()
    """
    log.debug(f'GET url: ``{build_request_url(api_url, params)}``')
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.get(api_url, params=params)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError('API response is not a JSON object.')

    return payload


def fetch_total_count(api_url: str, query: str, timeout: float) -> int:
    """
    Fetches the total number of matching public BDR records.

    Called by: main()
    """
    log.debug(f'api_url: ``{api_url}``')
    params: dict[str, str | int] = {
        'q': query,
        'rows': 0,
        'fl': 'pid',
    }
    log.debug(f'params: ``{pprint.pformat(params)}``')
    payload = fetch_search_payload(api_url, params, timeout)
    response_data = payload.get('response')
    if not isinstance(response_data, dict):
        raise ValueError('API response is missing a top-level "response" object.')

    total_count = response_data.get('numFound')
    if not isinstance(total_count, int):
        raise ValueError('API response is missing an integer "response.numFound" value.')

    return total_count


def parse_facet_values(payload: dict, facet_field: str) -> list[tuple[str, int]]:
    """
    Parses a Solr facet-field array into label/count pairs.

    Called by: fetch_facet_counts()
    """
    facet_counts = payload.get('facet_counts')
    if not isinstance(facet_counts, dict):
        raise ValueError('API response is missing a top-level "facet_counts" object.')

    facet_fields = facet_counts.get('facet_fields')
    if not isinstance(facet_fields, dict):
        raise ValueError('API response is missing "facet_counts.facet_fields".')

    raw_values = facet_fields.get(facet_field)
    if not isinstance(raw_values, list):
        raise ValueError(f'API response is missing facet data for "{facet_field}".')

    parsed_values: list[tuple[str, int]] = []
    index = 0
    while index < len(raw_values):
        label = raw_values[index]
        count = raw_values[index + 1]
        if isinstance(label, str) and isinstance(count, int):
            parsed_values.append((label, count))
        index += 2

    return parsed_values


def fetch_facet_counts(api_url: str, query: str, facet_field: str, top_n: int, timeout: float) -> list[tuple[str, int]]:
    """
    Fetches facet counts for a single Solr field.

    Called by: main()
    """
    params: dict[str, str | int] = {
        'q': query,
        'rows': 0,
        'fl': 'pid',
        'facet': 'true',
        'facet.field': facet_field,
        'facet.limit': top_n,
        'facet.mincount': 1,
    }
    payload = fetch_search_payload(api_url, params, timeout)
    facet_values = parse_facet_values(payload, facet_field)
    return facet_values


def extract_selected_counts(facet_counts: list[tuple[str, int]], labels: list[str]) -> list[tuple[str, int]]:
    """
    Extracts a subset of named facet counts in a stable display order.

    Called by: main()
    """
    counts_lookup = dict(facet_counts)
    selected_counts: list[tuple[str, int]] = []
    for label in labels:
        if label in counts_lookup:
            selected_counts.append((label, counts_lookup[label]))
    return selected_counts


def format_count(value: int) -> str:
    """
    Formats an integer count for display.

    Called by: render_report()
    """
    return f'{value:,}'


def render_report(
    total_count: int,
    resource_type_counts: list[tuple[str, int]],
    selected_object_type_counts: list[tuple[str, int]],
) -> str:
    """
    Renders the item-count report for stdout.

    Called by: main()
    """
    lines: list[str] = []
    lines.append(f'Total public non-collection records in the BDR: {format_count(total_count)}')
    lines.append('')
    lines.append('Rough breakdown by resource_type_ssi:')
    for label, count in resource_type_counts:
        lines.append(f'  {label}: {format_count(count)}')
    lines.append('')
    lines.append('Selected object_type counts:')
    for label, count in selected_object_type_counts:
        lines.append(f'  {label}: {format_count(count)}')
    report = '\n'.join(lines)
    return report


def main() -> None:
    """
    Fetches the total BDR item count and rough type breakdowns, then prints them.

    Called by: __main__
    """
    parser = build_parser()
    args = parser.parse_args()
    if args.top_n < 1:
        parser.error('--top-n must be greater than 0')

    try:
        total_count = fetch_total_count(args.api_url, args.query, args.timeout)
        resource_type_counts = fetch_facet_counts(
            args.api_url,
            args.query,
            DEFAULT_RESOURCE_TYPE_FIELD,
            args.top_n,
            args.timeout,
        )
        object_type_counts = fetch_facet_counts(
            args.api_url,
            args.query,
            DEFAULT_OBJECT_TYPE_FIELD,
            max(args.top_n, len(MEDIA_OBJECT_TYPES)),
            args.timeout,
        )
        selected_object_type_counts = extract_selected_counts(object_type_counts, MEDIA_OBJECT_TYPES)
        report = render_report(total_count, resource_type_counts, selected_object_type_counts)
    except (httpx.HTTPError, ValueError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        raise SystemExit(1) from exc

    print(report)


if __name__ == '__main__':
    main()
