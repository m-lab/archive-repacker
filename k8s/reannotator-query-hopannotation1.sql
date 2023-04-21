SELECT
  parser.ArchiveURL as ArchiveURL,
  [STRUCT("" as Filename, "" as DstIP)] as Files
FROM
  `measurement-lab.ndt_raw.hopannotation1`
WHERE
  date = @date
GROUP BY
  ArchiveURL
