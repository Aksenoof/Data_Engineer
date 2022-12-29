-- фамилия сотрудника с самой высокой зарплатой
SELECT fullname
FROM employees e
WHERE salary = (SELECT MAX(salary) 
				FROM employees e
                );

-- фамилии сотрудников в алфавитном порядке
SELECT fullname 
FROM employees e 
ORDER BY fullname;

-- средний стаж для каждого уровня сотрудников
SELECT level, AVG(AGE(CURRENT_DATE, start_date)) AS experience_mean
FROM employees e
GROUP BY level
ORDER BY experience_mean;

-- фамилия сотрудника и название отдела, в котором он работает
SELECT fullname, name AS department 
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.id;

-- название отдела и фамилия сотрудника с самой высокой зарплатой в данном отделе 
SELECT name AS department, fullname, salary
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.id
WHERE salary = (SELECT MAX(salary) 
				FROM employees e
                );

--название отдела, сотрудники которого получат наибольшую премию по итогам года
SELECT name AS department
FROM employees e
LEFT JOIN departments d
ON e.department_id = d.id
WHERE bonus = (SELECT MAX(bonus) 
			   FROM employees e
               );

-- индексация зарплат сотрудников с учетом коэффициента премии
SELECT id, fullname, salary, bonus AS bonus_coef, 
salary * bonus AS bonus, 
SUM(salary * b) AS index_bonus, 
SUM(salary * b) + salary AS index_salary,
SUM(salary * b) + salary + (salary * bonus) AS total_salary
FROM (SELECT *, CASE WHEN bonus > 1.2 THEN 0.2
					 WHEN (1 < bonus AND bonus <= 1.2)  THEN 0.1
			    else 0 END AS b
      FROM employees e
      ) AS b_y 
GROUP BY id, fullname, salary, bonus
ORDER BY id

-- отчет
WITH 
jun AS (SELECT d.id,COUNT(level) AS jun FROM employees e 
JOIN departments d
ON e.department_id = d.id
WHERE level = 'jun'
GROUP BY d.name, d.id),
middle AS (SELECT d.id, COUNT(level) AS middle FROM employees e 
JOIN departments d
ON e.department_id = d.id
WHERE level = 'middle'
GROUP BY d.name, d.id),
senior AS (SELECT d.id, COUNT(level) AS senior FROM employees e 
JOIN departments d
ON e.department_id = d.id
WHERE level = 'senior'
GROUP BY d.name, d.id),
lead AS (SELECT d.id, COUNT(level) AS lead FROM employees e 
JOIN departments d
ON e.department_id = d.id
WHERE level = 'lead'
GROUP BY d.name, d.id),
sum_index_salary  AS (SELECT department_id AS id, SUM(((salary * b) + salary)) AS sum_index_salary
FROM (SELECT *, CASE WHEN bonus > 1.2 THEN 0.2
					 WHEN (1 < bonus AND bonus <= 1.2)  THEN 0.1
			    else 0 END AS b
      FROM employees e
      ) AS b_y 
GROUP BY department_id),
grade_A AS (SELECT department_id, SUM(quarter1) + SUM(quarter2) + SUM(quarter3) + SUM(quarter4) AS grade_A
FROM(SELECT *, CASE WHEN q1 = 'A' THEN 1
			   ELSE 0 END quarter1,
	 		   CASE WHEN q2 = 'A' THEN 1
			   ELSE 0 END quarter2,
	 	 	   CASE WHEN q3 = 'A' THEN 1
			   ELSE 0 END quarter3,
	 	 	   CASE WHEN q4 = 'A' THEN 1
			   ELSE 0 END quarter4
	FROM scores s) AS b_q 
RIGHT JOIN employees e 
ON b_q.employee_id = e.id
GROUP BY department_id),
grade_B AS (SELECT department_id, SUM(quarter1) + SUM(quarter2) + SUM(quarter3) + SUM(quarter4) AS grade_B
FROM(SELECT *, CASE WHEN q1 = 'B' THEN 1
			   ELSE 0 END quarter1,
	 		   CASE WHEN q2 = 'B' THEN 1
			   ELSE 0 END quarter2,
	 	 	   CASE WHEN q3 = 'B' THEN 1
			   ELSE 0 END quarter3,
	 	 	   CASE WHEN q4 = 'B' THEN 1
			   ELSE 0 END quarter4
	FROM scores s) AS b_q 
RIGHT JOIN employees e 
ON b_q.employee_id = e.id
GROUP BY department_id),
grade_C AS (SELECT department_id, SUM(quarter1) + SUM(quarter2) + SUM(quarter3) + SUM(quarter4) AS grade_C
FROM(SELECT *, CASE WHEN q1 = 'C' THEN 1
			   ELSE 0 END quarter1,
	 		   CASE WHEN q2 = 'C' THEN 1
			   ELSE 0 END quarter2,
	 	 	   CASE WHEN q3 = 'C' THEN 1
			   ELSE 0 END quarter3,
	 	 	   CASE WHEN q4 = 'C' THEN 1
			   ELSE 0 END quarter4
	FROM scores s) AS b_q 
RIGHT JOIN employees e 
ON b_q.employee_id = e.id
GROUP BY department_id),
grade_D AS (SELECT department_id, SUM(quarter1) + SUM(quarter2) + SUM(quarter3) + SUM(quarter4) AS grade_D
FROM(SELECT *, CASE WHEN q1 = 'D' THEN 1
			   ELSE 0 END quarter1,
	 		   CASE WHEN q2 = 'D' THEN 1
			   ELSE 0 END quarter2,
	 	 	   CASE WHEN q3 = 'D' THEN 1
			   ELSE 0 END quarter3,
	 	 	   CASE WHEN q4 = 'D' THEN 1
			   ELSE 0 END quarter4
	FROM scores s) AS b_q 
RIGHT JOIN employees e 
ON b_q.employee_id = e.id
GROUP BY department_id),
grade_E AS (SELECT department_id, SUM(quarter1) + SUM(quarter2) + SUM(quarter3) + SUM(quarter4) AS grade_E
FROM(SELECT *, CASE WHEN q1 = 'E' THEN 1
			   ELSE 0 END quarter1,
	 		   CASE WHEN q2 = 'E' THEN 1
			   ELSE 0 END quarter2,
	 	 	   CASE WHEN q3 = 'E' THEN 1
			   ELSE 0 END quarter3,
	 	 	   CASE WHEN q4 = 'E' THEN 1
			   ELSE 0 END quarter4
	FROM scores s) AS b_q 
RIGHT JOIN employees e 
ON b_q.employee_id = e.id
GROUP BY department_id),
sum_bonus AS (SELECT department_id, SUM(salary * bonus) AS sum_bonus 
FROM  employees e
GROUP BY department_id),
total_salary AS (SELECT department_id, SUM(salary + (salary * bonus)) AS total_salary 
FROM  employees e
GROUP BY department_id)
SELECT name AS department, manager, number_employees, AVG(AGE(CURRENT_DATE, start_date)) AS experience_mean,
ROUND(AVG(salary),2) AS salary_mean, COALESCE(NULLIF(jun, null), 0) AS jun, COALESCE(NULLIF(middle, null), 0) AS middle,
COALESCE(NULLIF(senior, null), 0) AS senior, COALESCE(NULLIF(lead, null), 0) AS  lead, SUM(salary) AS  sum_salary, 
sum_index_salary, grade_A, grade_B, grade_C, grade_D, grade_E, ROUND(AVG(bonus),2) AS bonus_coef_mean, sum_bonus, total_salary, 
sum_index_salary + sum_bonus AS total_salary_index, 
ROUND(((((sum_index_salary + sum_bonus) - total_salary)/total_salary)*100), 2) AS difference
FROM employees e 
LEFT JOIN departments d ON e.department_id = d.id
LEFT JOIN jun j ON e.department_id = j.id
LEFT JOIN middle m ON e.department_id = m.id
LEFT JOIN senior s ON e.department_id = s.id
LEFT JOIN lead l ON e.department_id = l.id
LEFT JOIN sum_index_salary sis ON e.department_id = sis.id
LEFT JOIN grade_A g_A ON e.department_id = g_A.department_id
LEFT JOIN grade_B g_B ON e.department_id = g_B.department_id
LEFT JOIN grade_C g_C ON e.department_id = g_C.department_id
LEFT JOIN grade_D g_D ON e.department_id = g_D.department_id
LEFT JOIN grade_E g_E ON e.department_id = g_E.department_id
LEFT JOIN sum_bonus s_b ON e.department_id = s_b.department_id
LEFT JOIN total_salary t_s ON e.department_id = t_s.department_id
GROUP BY department, manager, number_employees, jun, middle, senior, lead, sum_index_salary, grade_A, grade_B, grade_C,
grade_D, grade_E, sum_bonus, total_salary, difference
ORDER BY department