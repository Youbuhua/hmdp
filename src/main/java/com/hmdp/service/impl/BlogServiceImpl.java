package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Autowired
    private IFollowService followService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }


    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog == null) {
            return Result.fail("笔记不存在");
        }

        queryBlogUser(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    // 查询blog的发布者信息
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    // 查询当前用户是否给blog点给赞
    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if(user == null) {
            // 未登录 无须展示是否点赞
            return ;
        }
        Long userId = user.getId();
        Double score = redisTemplate.opsForZSet().score(RedisConstants.BLOG_LIKED_KEY + blog.getId(), userId.toString());
        blog.setIsLike(score != null);
    }


    @Override
    public Result likeBolg(Long id) {
        UserDTO user = UserHolder.getUser();

        // 获取当前用户
        Long userId = user.getId();

        // 在redis中查询当前用户是否给当前blog点过赞
        // 使用 ZSet (ordered_set) score 为点赞时间
        Double score = redisTemplate.opsForZSet().score(RedisConstants.BLOG_LIKED_KEY + id, userId.toString());

        if(score != null) {
            // 点过赞
            // 取消点赞
            boolean success = update().setSql("liked = liked - 1").eq("id", id).update();
            if(success) {
                redisTemplate.opsForZSet().remove(RedisConstants.BLOG_LIKED_KEY + id, userId.toString());
            }
        }
        else {
            // 没点赞
            // 添加点赞信息
            boolean success = update().setSql("liked = liked + 1").eq("id", id).update();
            if(success) {
                redisTemplate.opsForZSet().add(RedisConstants.BLOG_LIKED_KEY + id, userId.toString(), System.currentTimeMillis());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 从Zset中读取时间最早的前五个 (0 - 4)
        Set<String> top5 = redisTemplate.opsForZSet().range(RedisConstants.BLOG_LIKED_KEY + id, 0, 4);
        if(top5 == null) {
            return Result.ok(Collections.emptyList());
        }

        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());

        String idStr = StrUtil.join(",", ids);
        if(StrUtil.isBlank(idStr)) {
            return Result.ok(Collections.emptyList());
        }

        List<UserDTO> userDTOList = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());

        return Result.ok(userDTOList);
    }

    // 新增 blog
    // blog存储到数据库
    // blog推送到关注者(粉丝)的收件箱
    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean success = save(blog);

        if(!success) {
            return Result.fail("新增blog失败");
        }

        // 将blog的id推送到关注者的收件箱
        // 获取关注者
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();

        for(Follow follow : follows) {
            Long userId = follow.getUserId();

            // 推送
            redisTemplate.opsForZSet().add(RedisConstants.FEED_KEY + userId, blog.getId().toString(), System.currentTimeMillis());
        }

        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogsOfFollow(Long max, Integer offset) {
        Long userId = UserHolder.getUser().getId();

        // 获取当前用户的收件箱 也就是被推送到的blog id集合
        // 使用滚动分页查询 记录上一次查询的最后位置
        Set<ZSetOperations.TypedTuple<String>> typedTuples = redisTemplate.opsForZSet().reverseRangeByScoreWithScores(RedisConstants.FEED_KEY + userId, 0, max, offset, 2);

        if(typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        // 用来存储收件箱中的blog的id
        List<Long> ids = new ArrayList<>(typedTuples.size());
        // 记录当前分页的最后一条的数据 用来在下一页查询
        long minTime = System.currentTimeMillis();
        // 记录当前分页的最后一条的数据 配合进行滚动分页
        Integer os = 1;
        for(ZSetOperations.TypedTuple<String> tuple: typedTuples) {
            // 获取blog的id
            String idStr = tuple.getValue();
            ids.add(Long.valueOf(idStr));

            // 获取blog的分数
            long time = tuple.getScore().longValue();
            if(time < minTime) {
                 minTime = time;
                 os = 1;
            }
            else if(time == minTime){
                os ++;
            }
        }

        String idStr = StrUtil.join(",", ids);
        if(StrUtil.isBlank(idStr)) {
            return Result.ok(Collections.emptyList());
        }

        // 根据id查询当前分页的blog
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        for(Blog blog: blogs) {
            queryBlogUser(blog);
            isBlogLiked(blog);
        }

        // 封装并返回 告知下一页应该从什么地方 多少偏移开始查
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setMinTime(minTime);
        r.setOffset(os);


        return Result.ok(r);
    }
}
