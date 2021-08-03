

# Introduction 
Repository pattern implementation base on file system. A Json file format will be used to store the data
This is very useful when working on POCs where you are going to use later other repository pattern implementations.

# Getting Started
Just get it and build it. I was using VS 2019

The idea is to have a separate IRepository for each domain object. 
The domain object will know nothing about the storage layer and the effort to build the storage layer is minimum.

# Build and Test
Building is easy since there it not special dependecies.

There are a bunch of integration tests that you can run without any extra configuration