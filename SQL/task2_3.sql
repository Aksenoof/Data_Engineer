CREATE TYPE level_type AS ENUM ('jun', 'middle', 'senior', 'lead');
CREATE TYPE grade_type AS ENUM ('A', 'B', 'C', 'D', 'E');

CREATE TABLE IF NOT EXISTS departments(
	id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	name VARCHAR(50) NOT NULL,
	manager VARCHAR(50),
	number_employees SMALLINT)

CREATE TABLE IF NOT EXISTS employees(
	id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	fullname VARCHAR(50) NOT NULL,
	dob DATE NOT NULL,
	start_date DATE NOT NULL,
	post VARCHAR(50) NOT NULL,
	level level_type NOT NULL,
	salary INT,
	department_id INT,
	driver_license BOOLEAN,
    CONSTRAINT department_fk
        FOREIGN KEY (department_id)
        REFERENCES departments(id)
        ON DELETE CASCADE)

CREATE TABLE IF NOT EXISTS scores(
	id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	employee_id INT,
	q1 grade_type,
	q2 grade_type,
	q3 grade_type,
	q4 grade_type,
	CONSTRAINT employee_fk
        FOREIGN KEY (employee_id)
        REFERENCES employees(id)
        ON DELETE CASCADE)

INSERT INTO departments(
	name,
	manager,
	number_employees
)
VALUES
	('developments', 'Иванов Н.П.', 6),
	('marketing', 'Фролова Ю.Н.', 2),
	('интеллектуальный анализ данных', 'Смирнов А.П.', 3);


INSERT INTO employees(
	fullname,
	dob,
	start_date,
	post,
	level,
	salary,
	department_id,
	driver_license
)
VALUES
	('Иванов Н.П.', '1980/05/12', '2010/02/13', 'руководитель отдела', 'lead', 385000, 1, true),
	('Петро Н.Ю.', '1985/04/22', '2012/07/23', 'программист python', 'senior', 310000, 1, true),
	('Сидоров С.С.', '1988/07/10', '2015/08/17', 'программист python', 'middle', 255000, 1, true),
	('Степанов И.В.', '1990/10/02', '2020/01/13', 'программист python', 'jun', 85000, 1, false),
	('Николаев В.В.', '1978/01/25', '2014/03/03', 'back-end разработчик', 'senior', 285000, 1, true),
	('Цой В.Р.', '1980/03/04', '2015/11/11', 'back-end разработчик', 'middle', 215000, 1, false),
	('Пушкин С.А.', '1991/05/12', '2021/12/09', 'back-end разработчик', 'jun', 115000, 1, false),
	('Фролова Ю.Н.', '1987/04/12', '2017/03/23', 'руководитель отдела', 'lead', 358000, 2, true),
	('Дмитреев К.Ю.', '1981/06/16', '2015/06/13', 'web-программист', 'senior', 210000, 2, true),
	('Злобин В.Ю.', '1988/07/27', '2019/04/14', 'web-программист', 'middle', 198000, 2, true),
	('Смирнов А.П.', '1983/03/28', '2021/01/11', 'руководитель отдела', 'lead', 347000, 3, true),
	('Зайцев И.Н.', '1977/05/25', '2021/02/28', 'data scientist', 'senior', 276000, 3, true),
	('Волков Н.П.', '1980/01/23', '2021/02/28', 'data scientist', 'middle', 203000, 3, false),
	('Сорока М.Ю.', '1990/09/03', '2021/04/03', 'data scientist', 'jun', 98000, 3, true);


INSERT INTO scores(
	employee_id,
	q1,
	q2,
	q3,
	q4
)
VALUES
	(1, 'C', 'B', 'C', 'B'),
	(2, 'C', 'C', 'C', 'B'),
	(3, 'B', 'B', 'D', 'B'),
	(4, 'B', 'A', 'C', 'E'),
	(5, 'C', 'C', 'C', 'B'),
	(6, 'C', 'C', 'C', 'C'),
	(7, 'B', 'B', 'C', 'B'),
	(8, 'C', 'B', 'D', 'B'),
	(9, 'C', 'D', 'C', 'B'),
	(10, 'C', 'B', 'C', 'B'),
	(11, 'D', 'B', 'B', 'E'),
	(12, 'D', 'B', 'C', 'B'),
	(13, 'C', 'C', 'C', 'B'),
	(14, 'C', 'B', 'B', 'B');


-- вывод id, фамилию и стаж сотрудника всего списка
SELECT id, fullname, AGE(CURRENT_DATE, start_date) AS  experience
FROM employees e;

-- вывод id, фамилию и стаж сотрудника первых трех сотрудников
SELECT id, fullname, AGE(CURRENT_DATE, start_date) AS  experience
FROM employees e ORDER BY id LIMIT 3;

-- список сотрудников водителей
SELECT id 
FROM employees e 
WHERE driver_license=true;

-- id сотрудников, которые хотя бы за 1 квартал получили оценку D или E
SELECT employee_id 
FROM scores s 
WHERE (q1='D' OR q1='E' OR 
	   q2='D' OR q2='E' OR
	   q3='D' OR q3='E' OR
	   q4='D' OR q4='E'
	   );

--масксимальная зарплата в компании
SELECT MAX(salary) 
FROM employees e;

-- название самого крупного отдела
SELECT name 
FROM departments d 
ORDER BY name LIMIT 1;

SELECT name 
FROM departments d 
WHERE number_employees=(SELECT MAX(number_employees) 
						FROM departments d);

-- номера сотрудников по уровню
SELECT id, level 
FROM employees e
ORDER BY CASE level WHEN 'lead' THEN 1
			WHEN 'senior' THEN 2
			WHEN 'middle' THEN 3
			WHEN 'jun' THEN 4
			END;

-- номера сотрудников от самых опытных до вновь прибывших
SELECT id FROM employees e 
ORDER BY AGE(CURRENT_DATE, start_date) DESC;

-- средняя зарплата для каждого уровня сотрудников
SELECT level, ROUND(AVG(salary),2) AS mean 
FROM employees e 
GROUP BY level 
ORDER BY mean DESC;

-- коэффициент годовой премии
ALTER TABLE employees ADD COLUMN bonus NUMERIC

UPDATE employees e
SET bonus = (SELECT 1 + quarter1 + quarter2 + quarter3 + quarter4 AS bonus_year 
			FROM (SELECT s.*, CASE q1 WHEN 'A' THEN 0.2
								WHEN 'B' THEN 0.1
								WHEN 'C' THEN 0
								WHEN 'D' THEN -0.1
								WHEN 'E' THEN -0.2
								else 1 END AS quarter1,
							  CASE q2 WHEN 'A' THEN 0.2
								WHEN 'B' THEN 0.1
								WHEN 'C' THEN 0
								WHEN 'D' THEN -0.1
								WHEN 'E' THEN -0.2
								else 1 END AS quarter2,
							  CASE q3 WHEN 'A' THEN 0.2
								WHEN 'B' THEN 0.1
								WHEN 'C' THEN 0
								WHEN 'D' THEN -0.1
								WHEN 'E' THEN -0.2
								else 1 END AS quarter3,
							   CASE q4 WHEN 'A' THEN 0.2
								WHEN 'B' THEN 0.1
								WHEN 'C' THEN 0
							    WHEN 'D' THEN -0.1
								WHEN 'E' THEN -0.2
								else 1 END AS quarter4 
				FROM scores s) AS b_q
			WHERE e.id = b_q.id								
			);