requires 'perl', '5.008001';
requires 'Net::Amazon::DynamoDB::Table';

on 'test' => sub {
    requires 'Test::More', '0.98';
};

