-- Insert data into people table
INSERT INTO people (name, email, phone_number) VALUES
('John Doe', 'john.doe@example.com', '123-456-7890'),
('Jane Smith', 'jane.smith@example.com', '098-765-4321'),
('Alice Johnson', 'alice.johnson@example.com', '234-567-8901'),
('Bob Brown', 'bob.brown@example.com', '345-678-9012'),
('Charlie Davis', 'charlie.davis@example.com', '456-789-0123'),
('David Green', 'david.green@example.com', '567-890-1234'),
('Eva White', 'eva.white@example.com', '678-901-2345'),
('Frank Black', 'frank.black@example.com', '789-012-3456'),
('Grace Blue', 'grace.blue@example.com', '890-123-4567'),
('Helen Purple', 'helen.purple@example.com', '901-234-5678');

-- Insert data into roles table
INSERT INTO roles (name, description) VALUES
('Designer', 'Designs user interfaces and experiences.'),
('Backend Developer', 'Develops server-side logic and APIs.'),
('Frontend Developer', 'Creates and manages web user interfaces.'),
('Data Engineer', 'Builds and maintains data pipelines.'),
('DevOps', 'Ensures smooth deployment and infrastructure.'),
('QA Engineer', 'Ensures software quality through testing.'),
('Product Manager', 'Oversees product development and strategy.'),
('UI/UX Designer', 'Focuses on user experience and interface design.'),
('Project Manager', 'Manages project timelines and deliverables.'),
('System Administrator', 'Maintains server infrastructure and networking.');

-- Delete data from people table
DELETE FROM people WHERE id = 5;
DELETE FROM people WHERE id = 8;


-- Delete data from roles table
DELETE FROM roles WHERE id = 1;
DELETE FROM roles WHERE id = 6;

-- Update data in people table
UPDATE people SET phone_number = '111-222-3333' WHERE id = 1;
UPDATE people SET email = 'jane.smith@newdomain.com' WHERE id = 2;
UPDATE people SET name = 'Alice Cooper' WHERE id = 3;
UPDATE people SET phone_number = '444-555-6666' WHERE id = 4;
UPDATE people SET name = 'Grace Green' WHERE id = 9;


-- Update data in roles table
UPDATE roles SET description = 'Handles the back-end database and logic.' WHERE id = 1;
UPDATE roles SET description = 'Manages both server-side and client-side code.' WHERE id = 2;
UPDATE roles SET description = 'Focuses on machine learning and data pipelines.' WHERE id = 4;
UPDATE roles SET description = 'Oversees all development and infrastructure processes.' WHERE id = 5;
UPDATE roles SET description = 'Responsible for maintaining and enhancing network systems.' WHERE id = 10;
