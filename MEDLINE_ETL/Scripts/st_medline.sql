-- DROP TABLE st_medline;

CREATE TABLE st_medline
(
	pmid 					int,
	"Year" 					varchar(10), 
	"Month" 				varchar(10), 
	"Day" 					varchar(10), 
	ISSN 					varchar(50), 
	Volume 					varchar(50), 
	Year_1 					varchar(10), 
	Month_1 				varchar(10), 
	Day_1 					varchar(10), 
	Title 					varchar(1200), 
	ISOAbbreviation  		varchar(1200), 
	ArticleTitle 			varchar(1200), 
	MedlinePgn  			varchar(50), 
	ELocationID 			varchar(300), 
	AbstractText 			text,
	CopyrightInformation  	varchar(1200), 
	LastName  				varchar(255), 
	ForeName  				varchar(255), 
	Initials  				varchar(10), 
	Affiliation 			text,
	AuthorList 				text,
	"Language" 				varchar(10), 
	PublicationType 		varchar(50), 
	Year_2 					varchar(10), 
	Month_2 				varchar(10), 
	Day_2 					varchar(10), 
	Country 				varchar(50), 
	MedlineTA 				varchar(255), 
	NlmUniqueID 			varchar(50), 
	ISSNLinking 			varchar(50), 
	CitationSubset 			varchar(10), 
	Keyword 				varchar(2550), 
	Year_3 					varchar(10), 
	Month_3 				varchar(10), 
	Day_3 					varchar(10), 
	"Hour" 					varchar(10), 
	"Minute" 				varchar(10), 
	History 				text,
	PublicationStatus  		varchar(50), 
	ArticleId  				varchar(50), 
	sub  					varchar(255) 
);

CREATE INDEX st_medline_pmid_idx ON  				medline.st_medline (pmid); 