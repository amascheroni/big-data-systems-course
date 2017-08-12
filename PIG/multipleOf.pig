/* Created By Maximiliano Agustin Mascheroni
   It count the multiples of a given number */ 

--get numbers from numbersInput file stored in HDFS
all_numbers = LOAD '/user/ubuntu/numbersInput' AS (number:int);

--get remainders of numbers at dividing them by $target
remainders = FOREACH all_numbers GENERATE (number%$target) AS (remainder:int);

--counting the multiples
multiplesof_filter = FILTER remainders BY (remainder==0);
multiplesof_group = GROUP multiplesof_filter ALL;
multipleof_count = FOREACH multiplesof_group GENERATE COUNT(multiplesof_filter) AS (result:long);

--generating the output file that will be stored in outputPig folder in HDFS
final_result = FOREACH multipleof_count GENERATE CONCAT('The count of multiples of $target is: ', (chararray)result);
STORE final_result INTO 'outputPig';
