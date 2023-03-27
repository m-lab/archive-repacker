-- query.sql -- Generates results suitable for the archive reannotator.

WITH archive_filename_ips AS (
  SELECT
    annotation.parser.ArchiveURL as ArchiveURL,
    annotation.parser.Filename,
    a.SockID.DstIP
  FROM
    `measurement-lab.ndt.tcpinfo` as tcpinfo LEFT JOIN `measurement-lab.raw_ndt.annotation` as annotation USING(id)
  WHERE
    tcpinfo.date = @date AND annotation.date = @date
  GROUP BY
    ArchiveURL, Filename, DstIP
)
SELECT
  ArchiveURL,
  ARRAY_AGG(STRUCT(Filename, DstIP)) as Files
FROM
  archive_filename_ips
GROUP BY
  ArchiveURL
ORDER BY
  ArchiveURL
