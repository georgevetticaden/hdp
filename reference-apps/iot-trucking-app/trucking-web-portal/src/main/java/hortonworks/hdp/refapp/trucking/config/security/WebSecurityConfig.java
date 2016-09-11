/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hortonworks.hdp.refapp.trucking.config.security;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@EnableWebSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

	private static Logger LOG = LoggerFactory.getLogger(WebSecurityConfig.class);

	
	@Override
	protected void configure(HttpSecurity http) throws Exception {
		http
			.csrf().disable() 
			.authorizeRequests()
				.antMatchers("/assets/**").permitAll()
				.anyRequest().authenticated()
				.and()
			.logout()
				.logoutSuccessUrl("/login?logout")
				.logoutUrl("/logout")
				.permitAll()
				.and()
			.formLogin()
				//.defaultSuccessUrl("/dashboard", true)
				.loginPage("/login")
				.loginProcessingUrl("/login")
				.failureUrl("/login?error")
				.permitAll().successHandler(new AuthSuccessHandler("/dashboard", true));
	}


	@Override
	protected void configure(AuthenticationManagerBuilder auth) throws Exception {
		auth.inMemoryAuthentication()
				.withUser("northcentral").password("northcentral").roles("USER");
		auth.inMemoryAuthentication().withUser("southcentral").password("southcentral").roles("USER");
	}
}