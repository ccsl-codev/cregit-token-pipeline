We present a dataset of open source software developed mainly by
enterprises rather than volunteers.  This can be used to address known
generalizability concerns, and, also, to perform research on open
source business software development.  Based on the premise that an
enterprise's employees are likely to contribute to a project developed
by their organization using the email account provided by it, we mine
domain names associated with enterprises from open data sources as
well as through white- and blacklisting, and use them through three
heuristics to identify 17,264 enterprise GitHub projects.  We provide
these as a dataset detailing their provenance and properties.  A manual
evaluation of a dataset sample shows an identification accuracy of 89%.
Through an exploratory data analysis we found that projects are staffed
by a plurality of enterprise insiders, who appear to be pulling more
than their weight, and that in a small percentage of relatively large
projects development happens exclusively through enterprise insiders.

The dataset is provided as a 17,264 record
tab-separated file with the following 29 fields.

* **url**: the project's GitHub URL
* **project_id**: the project's GHTorrent identifier
* **sdtc**: true if selected using the same domain top committers heuristic (9,016 records)
* **mcpc**: true if selected using the multiple committers from a valid enterprise heuristic (8,314 records)
* **mcve**: true if selected using the multiple committers from a probable company heuristic (8,015 records)
* **star_number**: number of GitHub watchers
* **commit_count**: number of commits
* **files**: number of files in current main branch
* **lines**: corresponding number of lines in text files
* **pull_requests**: number of pull requests
* **github_repo_creation**: timestamp of the GitHub repository creation
* **earliest_commit**: timestamp of the earliest commit
* **most_recent_commit**: timestamp of the most recent commit
* **committer_count**: number of different committers
* **author_count**: number of different authors
* **dominant_domain**: the project's dominant email domain
* **dominant_domain_committer_commits**: number of commits made by committers whose email matches the project's dominant domain
* **dominant_domain_author_commits**: corresponding number for commit authors
* **dominant_domain_committers**: number of committers whose email matches the project's dominant domain
* **dominant_domain_authors**: corresponding number for commit authors
* **cik**: SEC's EDGAR ``central index key''
* **fg500**: true if this is a Fortune Global 500 company (2,233 records)
* **sec10k**: true if the company files SEC 10-K forms (4,180 records)
* **sec20f**: true if the company files SEC 20-F forms (429 records)
* **project_name**: GitHub project name
* **owner_login**: GitHub project's owner login
* **company_name**: company name as derived from the SEC and Fortune 500 data
* **owner_company**: GitHub project's owner company name
* **license**: SPDX license identifier

The file `cohort_project_details.txt` provides the full set of
311,223 cohort projects that are not part of the enterprise data set,
but have comparable quality attributes.

* **url**: the project's GitHub URL
* **project_id**: the project's GHTorrent identifier
* **stars**: number of GitHub watchers
* **commit_count**: number of commits
