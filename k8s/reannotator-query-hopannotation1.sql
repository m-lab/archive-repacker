WITH parse_ips AS (
  SELECT parser, SPLIT(id, '_')[SAFE_OFFSET(2)] as DstIP
  FROM `measurement-lab.ndt_raw.hopannotation1`
  WHERE date = @date
)

SELECT
  parser.ArchiveURL as ArchiveURL,
  ARRAY_AGG(STRUCT(parser.Filename, DstIP)) as Files
FROM
  parse_ips
GROUP BY
  ArchiveURL
