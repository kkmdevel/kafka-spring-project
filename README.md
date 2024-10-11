# Kafka-Spring Project

A Spring Boot project for Kafka integration, demonstrating basic message production and consumption.

![Build Status](https://img.shields.io/travis/com/kkmdevel/kafka-spring-project)
![License](https://img.shields.io/github/license/kkmdevel/kafka-spring-project)

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/kkmdevel/kafka-spring-project.git
   ```
2. Navigate to the project directory:
   ```bash
   cd kafka-spring-project
   ```
3. Build and run the project using Maven:
   ```bash
   mvn clean install
   mvn spring-boot:run
   ```

### Prerequisites
- Java 11+
- Apache Kafka (local setup or cloud)
- Maven 3.6+

## Usage

After starting the application, you can produce messages to Kafka by making a POST request to:

```bash
curl -X POST http://localhost:8080/kafka/publish -d '{"message": "Hello, Kafka!"}'
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Open a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

If you have any questions, feel free to reach out at kkmdevel@example.com.
